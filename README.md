# ibge-sidra-tabelas

**Pipeline ETL robusto para baixar, normalizar e carregar tabelas agregadas do SIDRA/IBGE em PostgreSQL.**

---

Trabalhar com dados do IBGE é uma tarefa que todo analista e cientista de dados brasileiro conhece bem — e sabe que não é simples. A API SIDRA disponibiliza um acervo imenso de séries estatísticas (PIB municipal, população, inflação, agropecuária e muito mais), mas transformar esses dados brutos em um banco de dados relacional, limpo, normalizado e pronto para consulta é trabalhoso e cheio de armadilhas.

Este projeto resolve exatamente esse problema: um pipeline ETL completo, com controle de cache, downloads paralelos, carga em massa via protocolo COPY do PostgreSQL e um esquema de banco de dados cuidadosamente normalizado.

---

## Por que usar este projeto?

- **Zero redundância:** nomes de arquivo determinísticos garantem que a mesma requisição nunca seja baixada duas vezes.
- **Desempenho real:** downloads multi-threaded + carga via `COPY` do PostgreSQL são ordens de magnitude mais rápidos que abordagens ingênuas.
- **Confiabilidade:** retry com backoff exponencial lida com instabilidades da API sem interromper o pipeline.
- **Extensibilidade:** basta herdar `BaseScript` e declarar quais tabelas baixar — toda a orquestração é reutilizável.
- **Banco normalizado:** dados separados em quatro tabelas relacionais com constraints de unicidade e índices otimizados para consultas analíticas.

---

## Índice

- [Funcionalidades](#funcionalidades)
- [Arquitetura](#arquitetura)
- [Esquema do Banco de Dados](#esquema-do-banco-de-dados)
- [Séries Disponíveis](#séries-disponíveis)
- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Configuração](#configuração)
- [Uso](#uso)
  - [Executar um script](#executar-um-script)
  - [Executar todos os scripts](#executar-todos-os-scripts)
  - [Buscar apenas metadados](#buscar-apenas-metadados)
  - [Exportar dimensões para CSV](#exportar-dimensões-para-csv)
- [Fluxo de Dados](#fluxo-de-dados)
- [Referência dos Scripts](#referência-dos-scripts)
- [Módulos Internos](#módulos-internos)
- [Testes](#testes)

---

## Funcionalidades

| Funcionalidade | Detalhes |
|---|---|
| **Download paralelo** | Pool de threads configurável para baixar múltiplos períodos simultaneamente |
| **Cache inteligente** | Filenames determinísticos — cache-hit evita requisições duplicadas à API |
| **Retry com backoff** | Até 5 tentativas com delay exponencial (5s, 10s, 20s…) em falhas de rede |
| **Carga em massa** | Protocolo COPY nativo do PostgreSQL via `psycopg3` para inserção em alta performance |
| **Upsert idempotente** | `ON CONFLICT DO NOTHING/UPDATE` em todas as operações — re-execuções são seguras |
| **Normalização completa** | Localidades, dimensões (variável × classificação) e fatos em tabelas separadas |
| **Suporte a 6 classificações** | Produto cartesiano de até 6 níveis de classificação por variável |
| **Metadados persistidos** | Agregados, periodicidade e metadados JSON salvos no banco para consulta |
| **Logging detalhado** | Dual-channel (arquivo rotativo + console) com rastreamento de cada etapa |

---

## Arquitetura

O projeto segue uma arquitetura em camadas, com responsabilidades bem delimitadas:

```
┌─────────────────────────────────────────────────────────────┐
│                        Scripts ETL                          │
│           pibmunic.py  ·  ipca.py  ·  censo.py  · ...       │
│                  (herdam BaseScript)                        │
└──────────────────────────┬──────────────────────────────────┘
                           │ declara get_tabelas()
┌──────────────────────────▼──────────────────────────────────┐
│                       base.py                               │
│          Orquestração: download → metadata → load           │
└──────┬───────────────────┬──────────────────┬───────────────┘
       │                   │                  │
┌──────▼──────┐   ┌────────▼───────┐   ┌──────▼──────────────┐
│  sidra.py   │   │  database.py   │   │    storage.py       │
│  (Fetcher)  │   │ (load, upsert, │   │ (filesystem, cache, │
│  API client │   │  DDL builders) │   │  filename hashing)  │
└──────┬──────┘   └────────┬───────┘   └──────┬──────────────┘
       │                   │                  │
┌──────▼──────┐   ┌────────▼───────┐   ┌──────▼──────┐
│  SIDRA API  │   │  PostgreSQL    │   │ Sistema de  │
│  (IBGE)     │   │  (4 tabelas)   │   │ arquivos    │
└─────────────┘   └────────────────┘   └─────────────┘
```

**Princípios de design:**

- **Determinismo:** o mesmo conjunto de parâmetros sempre gera o mesmo nome de arquivo — re-execuções são seguras e baratas.
- **Dois passos de carga:** o primeiro escaneamento coleta chaves únicas de localidades e dimensões; o segundo transmite os dados via COPY, evitando acúmulo em memória.
- **Abstração via BaseScript:** scripts concretos definem apenas `get_tabelas()` — toda a lógica de pipeline é herdada.

---

## Esquema do Banco de Dados

O banco é organizado em quatro tabelas no schema `ibge_sidra` (configurável):

```
┌─────────────────┐       ┌──────────────────────────────────────────┐
│  sidra_tabela   │       │              dados (fatos)               │
│─────────────────│       │──────────────────────────────────────────│
│ id (PK)         │◄──────│ sidra_tabela_id (FK)                     │
│ nome            │       │ localidade_id (FK) ──────────────────────┼──►┌─────────────────┐
│ periodicidade   │       │ dimensao_id (FK) ────────────────────────┼──►│   localidade    │
│ metadados (JSON)│       │ d3c  (período, ex: "202301")             │   │─────────────────│
│ ultima_atualizac│       │ v    (valor numérico ou NULL)            │   │ id (PK)         │
└─────────────────┘       │ modificacao (timestamp)                  │   │ nc  (nível id)  │
                          │ ativo (boolean)                          │   │ nn  (nível nome)│
                          └──────────────────────────────────────────┘   │ d1c (unidade id)│
                                                                         │ d1n (unidade nom│
                          ┌──────────────────────────────────────────┐   └─────────────────┘
                          │              dimensao                    │
                          │──────────────────────────────────────────│
                          │ id (PK)                                  │
                          │ mc,mn  (unidade de medida id/nome)       │
                          │ d2c,d2n (variável id/nome)               │
                          │ d4c–d9c (ids das classificações, ≤6)     │
                          │ d4n–d9n (nomes das classificações)       │
                          └──────────────────────────────────────────┘
```

**Constraint de unicidade na tabela `dados`:**
```sql
UNIQUE (sidra_tabela_id, localidade_id, dimensao_id, d3c)
```

Isso garante que cada combinação de tabela × localidade × variável/classificação × período exista apenas uma vez, tornando re-execuções completamente seguras.

---

## Séries Disponíveis

Os scripts incluídos cobrem as principais pesquisas do IBGE:

| Pasta/Script | Pesquisa | Tabelas SIDRA |
|---|---|---|
| `scripts/pibmunic.py` | **PIB dos Municípios** | 5938 |
| `scripts/populacao/estimapop.py` | **Estimativas de População** | 6579 |
| `scripts/populacao/censo_populacao.py` | **Censo Demográfico** | 22, 23 |
| `scripts/populacao/contagem_populacao.py` | **Contagem de População** | 2951, 2952 |
| `scripts/snpc/ipca.py` | **IPCA** (índice de inflação) | 1692, 1693, 58, 61, 655, 656, 2938, 1419, 7060 |
| `scripts/snpc/ipca15.py` | **IPCA-15** | múltiplas tabelas |
| `scripts/snpc/inpc.py` | **INPC** | múltiplas tabelas |
| `scripts/ppm/rebanhos.py` | **PPM — Rebanhos** | 74, 3940 |
| `scripts/ppm/producao.py` | **PPM — Produção animal** | múltiplas tabelas |
| `scripts/ppm/exploracao.py` | **PPM — Exploração** | múltiplas tabelas |
| `scripts/pam/lavouras_temporarias.py` | **PAM — Lavouras temporárias** | 839, 1000, 1001, 1002, 1612 |
| `scripts/pam/lavouras_permanentes.py` | **PAM — Lavouras permanentes** | múltiplas tabelas |
| `scripts/pevs/producao.py` | **PEVS — Produção florestal** | múltiplas tabelas |
| `scripts/pevs/area_florestal.py` | **PEVS — Área florestal** | múltiplas tabelas |

---

## Pré-requisitos

- **Python 3.13+**
- **PostgreSQL 14+** (com usuário e banco de dados criados)
- Acesso à internet para consultar a API SIDRA do IBGE
- Biblioteca [`sidra-fetcher`](https://github.com/dankkom/sidra-fetcher) (instalada automaticamente via `pyproject.toml`)

---

## Instalação

```bash
# 1. Clone o repositório
git clone https://github.com/dankkom/ibge-sidra-tabelas.git
cd ibge-sidra-tabelas

# 2. Crie e ative o ambiente virtual
python -m venv .venv
source .venv/bin/activate       # Linux/macOS
# .venv\Scripts\activate        # Windows

# 3. Instale as dependências
pip install -e .
```

**Dependências principais:**

| Pacote | Uso |
|---|---|
| [`sidra-fetcher`](https://github.com/dankkom/sidra-fetcher) | Cliente HTTP para a API SIDRA do IBGE |
| `psycopg[binary] >= 3.2.9` | Adaptador PostgreSQL com extensões C |
| `sqlalchemy >= 2.0.41` | ORM e geração de SQL |
| `orjson >= 3.11.7` | Serialização JSON de alta performance |

---

## Configuração

Crie o arquivo `config.ini` na raiz do projeto:

```ini
[storage]
# Diretório onde os arquivos JSON baixados serão armazenados
data_dir = data

[database]
user       = postgres
password   = sua_senha
host       = localhost
port       = 5432
dbname     = dados
schema     = ibge_sidra
tablespace = pg_default
readonly_role = readonly_role
```

> **Nota:** O schema `ibge_sidra` será criado automaticamente na primeira execução, incluindo todas as tabelas, índices e constraints.

---

## Uso

### Executar um script

Cada script baixa e carrega no banco os dados de uma pesquisa específica:

```bash
# PIB dos Municípios
python scripts/pibmunic.py

# IPCA
python scripts/snpc/ipca.py

# Estimativas de população
python scripts/populacao/estimapop.py

# Lavouras temporárias (PAM)
python scripts/pam/lavouras_temporarias.py
```

### Executar todos os scripts

```bash
# Roda todos os scripts em scripts/ sequencialmente
./run-all.sh scripts
```

O script registra o código de saída de cada execução e continua mesmo em caso de falha individual.

### Buscar apenas metadados

Útil para inspecionar os metadados de uma tabela SIDRA antes de baixar os dados:

```bash
python fetch-metadata.py 5938
```

Salva o JSON de metadados localmente e persiste no banco de dados.

### Exportar dimensões para CSV

Gera um CSV com todas as combinações de variável × classificação de uma tabela:

```bash
python export-dimensao.py 5938 --output dimensoes_pib.csv
```

O arquivo gerado contém todas as dimensões possíveis com seus respectivos códigos e nomes, útil para documentação e exploração dos dados.

---

## Fluxo de Dados

```
API SIDRA (IBGE)
      │
      │  GET /agregados/{tabela}/periodos/{período}/...
      ▼
┌────────────────────────────────────────────────────┐
│  Fetcher (sidra.py)                                │
│  ┌─────────────────────────────────────────────┐   │
│  │  ThreadPoolExecutor (max_workers=4)         │   │
│  │  → download de cada período em paralelo     │   │
│  │  → retry com backoff (5 tentativas, base 5s)│   │
│  └──────────────────┬──────────────────────────┘   │
└─────────────────────┼──────────────────────────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │  Storage (storage.py)  │
         │  data/t-{id}/          │
         │  ├── arquivo1.json     │  ← nome determinístico
         │  ├── arquivo2.json     │    (tabela+período+terr+
         │  └── ...               │     vars+classif+mod)
         └───────────┬────────────┘
                     │
                     ▼
         ┌────────────────────────────────────────┐
         │  load_dados (database.py)              │
         │                                        │
         │  Passo 1: scan JSON                    │
         │  → coleta chaves únicas de             │
         │    localidades e dimensões             │
         │  → upsert em lotes de 5.000 linhas     │
         │  → constrói lookup dicts em memória    │
         │                                        │
         │  Passo 2: scan JSON novamente          │
         │  → resolve IDs via lookup              │
         │  → stream via COPY para staging table  │
         │  → INSERT com ON CONFLICT DO NOTHING   │
         └───────────────┬────────────────────────┘
                         │
                         ▼
               ┌──────────────────┐
               │   PostgreSQL     │
               │                  │
               │  sidra_tabela    │
               │  localidade      │
               │  dimensao        │
               │  dados           │
               └──────────────────┘
```

---

## Referência dos Scripts

### Criando um script personalizado

Basta herdar `BaseScript` e implementar `get_tabelas()`:

```python
from ibge_sidra_tabelas.base import BaseScript

class MeuScript(BaseScript):
    def get_tabelas(self):
        yield {
            "sidra_tabela": 5938,          # ID da tabela no SIDRA
            "territories": {6: ["all"]},   # nível 6 = municípios, "all" = todos
            "variables": [37, 498, 513],   # IDs das variáveis
            "classifications": {},         # classificações adicionais (opcional)
        }

if __name__ == "__main__":
    MeuScript().run()
```

O método `run()` executa automaticamente toda a sequência:
1. Cria as tabelas no banco (idempotente)
2. Busca e persiste os metadados
3. Baixa todos os períodos disponíveis (com cache)
4. Carrega os dados no PostgreSQL

### Parâmetros de `get_tabelas()`

| Chave | Tipo | Descrição |
|---|---|---|
| `sidra_tabela` | `int` | ID da tabela no SIDRA (ex: `5938`) |
| `territories` | `dict[int, list[str]]` | Nível territorial → lista de IDs ou `["all"]` |
| `variables` | `list[int]` | IDs das variáveis a baixar. Use `["all"]` para todas |
| `classifications` | `dict` | Classificações e categorias. Vazio para sem filtro |

**Níveis territoriais comuns:**

| Código | Descrição |
|---|---|
| `1` | Brasil |
| `2` | Grandes Regiões |
| `3` | Unidades da Federação |
| `6` | Municípios |
| `7` | Regiões Metropolitanas |

---

## Módulos Internos

### `config.py` — Gerenciamento de configuração

Lê `config.ini` e expõe credenciais do banco, diretório de dados e opções de logging.

```python
from ibge_sidra_tabelas.config import Config
config = Config("config.ini")
print(config.database.host)   # "localhost"
print(config.storage.data_dir) # "data"
```

### `sidra.py` — Cliente da API SIDRA

```python
from ibge_sidra_tabelas.sidra import Fetcher

with Fetcher(storage, config=config) as fetcher:
    filepaths = fetcher.download_table(
        sidra_tabela=5938,
        territories={6: ["all"]},
        variables=[37, 498],
        classifications={},
    )
```

O `Fetcher` gerencia internamente:
- Pool de threads para downloads paralelos
- Detecção de cache-hit (evita re-download)
- Retry com backoff exponencial em falhas de rede

### `storage.py` — Armazenamento em arquivo

Nomes de arquivo são gerados deterministicamente a partir dos parâmetros da requisição:

```
t5938_p202301_f3_n6-all_v37.498_c0_m1717200000.json
│     │        │  │       │      │  │
│     │        │  │       │      │  └─ timestamp de modificação
│     │        │  │       │      └──── classificações
│     │        │  │       └─────────── variáveis
│     │        │  └─────────────────── nível territorial
│     │        └────────────────────── formato
│     └─────────────────────────────── período
└───────────────────────────────────── tabela
```

### `database.py` — Operações no banco

```python
from ibge_sidra_tabelas.database import get_engine, load_dados

engine = get_engine(config)

# Carrega todos os arquivos de uma tabela
load_dados(engine, storage, data_files)
```

A carga usa o protocolo COPY do PostgreSQL via `psycopg3`, com inserção em tabela de staging e resolução de conflitos via `ON CONFLICT DO NOTHING`.

### `utils.py` — Utilitários de transformação

```python
from ibge_sidra_tabelas.utils import unnest_dimensoes

# Expande variáveis × classificações em produto cartesiano
dimensoes = list(unnest_dimensoes(variaveis, classificacoes))
```

Gera todas as combinações possíveis de variável × categoria de classificação, com resolução de unidade de medida (categoria > variável).

---

## Testes

```bash
pytest -q
```

A suíte de testes cobre:

| Arquivo | O que testa |
|---|---|
| `tests/test_config.py` | Carregamento de config, setup de logging |
| `tests/test_storage.py` | Geração de nomes, leitura/escrita, caminhos de metadados |
| `tests/test_base.py` | Cache de metadados, deduplicação, attachamento de filepaths |
| `tests/test_sidra.py` | Retry logic, unnesting de classificações, context manager |
| `tests/test_database.py` | Limpeza de dados, criação de engine, builders DDL/DCL |
| `tests/test_utils.py` | Produto cartesiano de dimensões, resolução de unidade |

---

## Estrutura do Repositório

```
ibge-sidra-tabelas/
├── src/ibge_sidra_tabelas/
│   ├── __init__.py
│   ├── base.py          # BaseScript — orquestração do pipeline
│   ├── config.py        # Leitura de config.ini
│   ├── database.py      # SQLAlchemy, carga, DDL/DCL
│   ├── models.py        # ORM models (tabelas, localidades, dimensões, dados)
│   ├── sidra.py         # Cliente da API SIDRA com retry e cache
│   ├── storage.py       # Filesystem: leitura, escrita, filenames
│   └── utils.py         # Produto cartesiano de dimensões
├── scripts/
│   ├── pibmunic.py
│   ├── populacao/
│   ├── snpc/            # IPCA, IPCA-15, INPC
│   ├── ppm/             # Pesquisa Pecuária Municipal
│   ├── pam/             # Produção Agrícola Municipal
│   └── pevs/            # Produção da Extração Vegetal e Silvicultura
├── tests/
├── fetch-metadata.py    # Utilitário: buscar metadados de uma tabela
├── export-dimensao.py   # Utilitário: exportar dimensões para CSV
├── run-all.sh           # Runner: executar todos os scripts
├── config.ini           # Configurações (não versionado)
└── pyproject.toml       # Metadados e dependências do projeto
```

---

## Licença

GNU GPLv3
