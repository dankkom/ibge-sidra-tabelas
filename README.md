# ibge-sidra-tabelas — Tabelas agregadas do IBGE

Projeto para baixar, padronizar e carregar tabelas agregadas do SIDRA
(IBGE) em um banco de dados PostgreSQL. Fornece utilitários para:

- consultar a API SIDRA e salvar CSVs por tabela/período;
- gerar nomes de arquivo determinísticos para requests;
- ler e refinar os CSVs para carga em banco;
- construir DDL/DCL simples para criação de tabelas e permissões.

Principais componentes
- `src/ibge_sidra_tabelas/sidra.py`: cliente de alto nível para baixar
	tabelas usando `sidra_fetcher`.
- `src/ibge_sidra_tabelas/storage.py`: helpers de leitura/gravação e
	geração de nomes de arquivo.
- `src/ibge_sidra_tabelas/base.py`: base abstrata `BaseScript` para
	pipelines de download + carga.
- `src/ibge_sidra_tabelas/database.py`: criação de engine, helpers de
	carga e builders de DDL/DCL.

Requisitos
- Python 3.10+
- Dependências listadas em `pyproject.toml` (instale com ``pip install -e .``)

Instalação rápida

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Configuração
---------------
Configure as credenciais do banco e outras opções em `config.ini` na
raiz do projeto. As chaves esperadas incluem credenciais de banco
(`db_user`, `db_password`, `db_host`, `db_port`, `db_name`), além do
nome da tabela/esquema de destino e do diretório de dados.

Uso
-----
Os scripts específicos estão na pasta `scripts/`. Em geral o fluxo é:

1. Declarar quais tabelas baixar (ver `scripts/` existentes).
2. Executar o script que usa `BaseScript` para baixar e carregar os
	 dados.

Exemplo (genérico):

```bash
python scripts/pibmunic.py
```

Formato de dados
-----------------
Os CSVs baixados ficam em `data/raw/ibge-tabelas/t-<tabela>/` e usam um
nome determinístico contendo tabela, períodos, formato, níveis
territoriais, variáveis, classificações e o timestamp de modificação.

Desenvolvimento e testes
------------------------
Execute os testes com `pytest`:

```bash
pytest -q
```
