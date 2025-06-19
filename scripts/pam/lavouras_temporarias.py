"""Produção Agrícola Municipal - Lavouras temporarias

Tabela 839 - Área plantada, área colhida, quantidade produzida e rendimento
             médio de milho, 1ª e 2ª safras (Vide Notas)

https://sidra.ibge.gov.br/tabela/839

Notas:

1 - Subentende a possibilidade de cultivos sucessivos ou simultâneos (simples,
    associados e/ou intercalados) no mesmo ano e no mesmo local, podendo, por
    isto, a área informada da cultura exceder a área geográfica do município.

2 - A diferença entre a área plantada e a área colhida na lavoura temporária é
    considerada como área perdida.

3 - Os dados das safras 2009 e 2010 da UF São Paulo não estão disponíveis, pois
    não foi possível obter informações detalhadas destas safras.

4 - Os dados do último ano divulgado são RESULTADOS PRELIMINARES e podem sofrer
    alterações até a próxima divulgação.

---

Tabela 1000 - Área plantada, área colhida, quantidade produzida e rendimento
              médio de amendoim, 1ª e 2ª safras (Vide Notas)

Notas:

1 - Subentende a possibilidade de cultivos sucessivos ou simultâneos (simples,
    associados e/ou intercalados) no mesmo ano e no mesmo local, podendo, por
    isto, a área informada da cultura exceder a área geográfica do município.

2 - A diferença entre a área plantada e a área colhida na lavoura temporária é
    considerada como área perdida.

3 - Os dados do último ano divulgado são RESULTADOS PRELIMINARES e podem sofrer
    alterações até a próxima divulgação.

---

Tabela 1001 - Área plantada, área colhida, quantidade produzida e rendimento
              médio de batata-inglesa, 1ª, 2ª e 3ª safras (Vide Notas)

Notas:

1 - Subentende a possibilidade de cultivos sucessivos ou simultâneos (simples,
    associados e/ou intercalados) no mesmo ano e no mesmo local, podendo, por
    isto, a área informada da cultura exceder a área geográfica do município.

2 - A diferença entre a área plantada e a área colhida na lavoura temporária é
    considerada como área perdida.

3 - Os dados do último ano divulgado são RESULTADOS PRELIMINARES e podem sofrer
    alterações até a próxima divulgação.

---

Tabela 1002 - Área plantada, área colhida, quantidade produzida e rendimento
              médio de feijão, 1ª, 2ª e 3ª safras (Vide Notas)

Notas:

1 - Subentende a possibilidade de cultivos sucessivos ou simultâneos (simples,
    associados e/ou intercalados) no mesmo ano e no mesmo local, podendo, por
    isto, a área informada da cultura exceder a área geográfica do município.

2 - A diferença entre a área plantada e a área colhida na lavoura temporária é
    considerada como área perdida.

3 - Os dados do último ano divulgado são RESULTADOS PRELIMINARES e podem sofrer
    alterações até a próxima divulgação.

---

Tabela 1612 - Área plantada, área colhida, quantidade produzida, rendimento
              médio e valor da produção das lavouras temporárias (Vide Notas)

Notas:

1 - Os municípios sem informação para pelo menos um produto da lavoura
    temporária não aparecem nas listas;

2 - A partir do ano de 2001 as quantidades produzidas dos produtos melancia e
    melão passam a ser expressas em toneladas. Nos anos anteriores eram
    expressas em mil frutos. O rendimento médio passa a ser expresso em Kg/ha.
    Nos anos anteriores era expresso em frutos/ha.

3 - Veja o documento AlteracoesUnidadesMedidaFrutas.pdf com as alterações de
    unidades de medida das frutíferas ocorridas em 2001 e a tabela de conversão
    fruto x quilograma.

4 - Os produtos girassol e triticale só apresentam informação a partir de 2005.

5 - A quantidade produzida de abacaxi é expressa em mil frutos e o rendimento
    médio em frutos/ha.

6 - Valores para a categoria Total indisponíveis para as variáveis Quantidade
    produzida e Rendimento médio, pois as unidades de medida diferem para
    determinados produtos.

7 - Subentende a possibilidade de cultivos sucessivos ou simultâneos (simples,
    associados e/ou intercalados) no mesmo ano e no mesmo local, podendo, por
    isto, a área informada da cultura exceder a área geográfica do município.

8 - As culturas de abacaxi, cana-de-açúcar, mamona e mandioca são consideradas
    culturas temporárias de longa duração. Elas costumam ter ciclo vegetativo
    que ultrapassa 12 meses e, por isso, as informações são computadas nas
    colheitas realizadas dentro de cada ano civil (12 meses). Nestas culturas a
    área plantada refere-se a área destinada à colheita no ano.

9 - A diferença entre a área plantada e a área colhida na lavoura temporária é
    considerada como área perdida.

10 - A variável Área plantada só passou a ser informada a partir de 1988.

11 - Valor da produção: Variável derivada calculada pela média ponderada das
     informações de quantidade e preço médio corrente pago ao produtor, de
     acordo com os períodos de colheita e comercialização de cada produto. As
     despesas de frete, taxas e impostos não são incluídas no preço.

12 - Os dados do último ano divulgado são RESULTADOS PRELIMINARES e podem
     sofrer alterações até a próxima divulgação.

Fonte: IBGE - Produção Agrícola Municipal

"""

from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_sidra_tabelas import database, sidra, storage
from ibge_sidra_tabelas.config import Config


def get_tabelas(fetcher: sidra.Fetcher):
    tabelas = (
        {
            "sidra_tabela": "839",
            "territories": {"6": []},
            "variables": ["allxp"],
            "classifications": {
                "81": ["allxt"]
            },  # Produto das lavouras temporárias
        },
        {
            "sidra_tabela": "1000",
            "territories": {"6": []},
            "variables": ["allxp"],
            "classifications": {
                "81": ["allxt"]
            },  # Produto das lavouras temporárias
        },
        {
            "sidra_tabela": "1001",
            "territories": {"6": []},
            "variables": ["allxp"],
            "classifications": {
                "81": ["allxt"]
            },  # Produto das lavouras temporárias
        },
    )
    tabelas_1002 = tuple(
        {
            "sidra_tabela": "1002",
            "territories": {"6": []},
            "variables": [variable],
            "classifications": {
                "81": ["allxt"]
            },  # Produto das lavouras temporárias
        }
        for variable in ("109", "216", "214", "112")
    )
    metadados_1612 = fetcher.sidra_client.get_agregado_metadados("1612")
    tabelas_1612 = tuple(
        {
            "sidra_tabela": "1612",
            "territories": {"6": []},
            "variables": ["allxp"],
            "classifications": classificacoes,  # Produto das lavouras temporárias
        }
        for classificacoes in sidra.unnest_classificacoes(
            metadados_1612.classificacoes
        )
    )
    return tabelas + tabelas_1002 + tabelas_1612


def download(
    fetcher: sidra.Fetcher, tabelas: list[dict[str, str]]
) -> list[Path]:
    filepaths = []
    for tabela in tabelas:
        _filepaths = fetcher.download_table(**tabela)
        filepaths.extend(_filepaths)
    return filepaths


def create_table(engine: sa.engine.Engine, config: Config):
    ddl = database.build_ddl(
        schema=config.db_schema,
        table_name=config.db_table,
        tablespace=config.db_tablespace,
        columns={
            "ano": "SMALLINT NOT NULL",
            "id_municipio": "TEXT NOT NULL",
            "produto": "TEXT NOT NULL",
            "variavel": "TEXT NOT NULL",
            "unidade": "TEXT NOT NULL",
            "valor": "DOUBLE PRECISION",
        },
        primary_keys=("ano", "id_municipio", "produto", "variavel"),
    )
    dcl = database.build_dcl(
        schema=config.db_schema,
        table_name=config.db_table,
        table_owner=config.db_user,
        table_user=config.db_readonly_role,
    )
    with Session(engine) as session:
        session.execute(sa.text(ddl))
        session.execute(sa.text(dcl))
        session.commit()


def refine(df):
    columns_rename = {
        "Ano (Código)": "ano",
        "Município (Código)": "id_municipio",
        "Produto das lavouras temporárias": "produto",
        "Variável": "variavel",
        "Unidade de Medida": "unidade",
        "Valor": "valor",
    }
    df = df[list(columns_rename.keys())]
    df = df.rename(columns=columns_rename)
    df = df.astype({"ano": int, "id_municipio": str})
    return df


def main():
    with sidra.Fetcher() as fetcher:
        tabelas = get_tabelas(fetcher=fetcher)
        filepaths = download(fetcher=fetcher, tabelas=tabelas)

    db_table = "pam_lavouras_temporarias"
    config = Config(db_table)
    engine = database.get_engine(config)
    create_table(engine, config)

    for filepath in filepaths:
        df = storage.read_file(filepath)
        df = refine(df)
        database.load(df, engine=engine, config=config)


if __name__ == "__main__":
    main()
