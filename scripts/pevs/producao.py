"""Produção da Extração Vegetal e da Silvicultura

Tabela 289 - Quantidade produzida e valor da produção na extração vegetal, por
             tipo de produto extrativo (Vide Notas)

https://sidra.ibge.gov.br/tabela/289

Notas:

1 - Os municípios sem informação para pelo menos um produto da extração vegetal
    não aparecem nas listas.

2 - Até 2001 era pesquisada a erva-mate cancheada. A partir de 2002 passou-se a
    pesquisar a erva-mate folha verde.

3 - Valor da produção na extração vegetal: Variável derivada calculada pela
    média ponderada das informações de quantidade e preço médio corrente pago
    ao produtor, de acordo com os períodos de colheita e comercialização de
    cada produto. As despesas de frete, taxas e impostos não são incluídas no
    preço.

4 - Os dados do último ano divulgado são RESULTADOS PRELIMINARES e podem sofrer
    alterações até a próxima divulgação.

---

Tabela 291 - Quantidade produzida e valor da produção na silvicultura, por tipo
             de produto da silvicultura (Vide Notas)

https://sidra.ibge.gov.br/tabela/291

Notas:

1 - As Unidades da Federação, mesorregiões, microrregiões e municípios sem
    informação para pelo menos um produto da silvicultura em pelo menos um ano
    da pesquisa não aparecem nas listas.

2 - Valor da produção na silvicultura: Variável derivada calculada pela média
    ponderada das informações de quantidade e preço médio corrente pago ao
    produtor, de acordo com os períodos de colheita e comercialização de cada
    produto. As despesas de frete, taxas e impostos não são incluídas no preço.

3 - Os dados do último ano divulgado são RESULTADOS PRELIMINARES e podem sofrer
    alterações até a próxima divulgação.

Fonte: IBGE - Produção da Extração Vegetal e da Silvicultura

"""

from typing import Any, Iterable

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_sidra_tabelas import database, sidra, storage
from ibge_sidra_tabelas.config import Config


def get_tabelas() -> Iterable[dict[str, Any]]:
    metadados_289 = sidra.get_metadados("289")
    tabelas_289 = tuple(
        {
            "sidra_tabela": "289",
            "territorial_level": "6",
            "ibge_territorial_code": "all",
            "variable": "allxp",
            "classifications": classificacoes,  # Tipo de produto extrativo
        }
        for classificacoes in sidra.unnest_classificacoes(metadados_289["classificacoes"], {})
    )
    metadados_291 = sidra.get_metadados("291")
    tabelas_291 = tuple(
        {
            "sidra_tabela": "291",
            "territorial_level": "6",
            "ibge_territorial_code": "all",
            "variable": "allxp",
            "classifications": classificacoes,  # Tipo de produto da silvicultura
        }
        for classificacoes in sidra.unnest_classificacoes(metadados_291["classificacoes"], {})
    )
    return tabelas_289 + tabelas_291


def download(tabelas: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    data_files = []
    for tabela in tabelas:
        _filepaths = sidra.download_table(**tabela)
        for filepath in _filepaths:
            data_files.append(tabela | {"filepath": filepath})
    return data_files


def create_table(engine: sa.Engine, config: Config):
    ddl = database.build_ddl(
        schema=config.db_schema,
        table_name=config.db_table,
        tablespace=config.db_tablespace,
        columns={
            "ano": "SMALLINT NOT NULL",
            "id_municipio": "TEXT NOT NULL",
            "grupo_produto": "TEXT NOT NULL",
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


def refine(df: pd.DataFrame) -> pd.DataFrame:
    columns_rename = {
        "Ano (Código)": "ano",
        "Município (Código)": "id_municipio",
        "Variável": "variavel",
        "Unidade de Medida": "unidade",
        "Valor": "valor",
    }
    if "Tipo de produto extrativo" in df.columns:
        columns_rename |= {"Tipo de produto extrativo": "produto"}
        grupo_produto = "Extração vegetal"
    elif "Tipo de produto da silvicultura" in df.columns:
        columns_rename |= {"Tipo de produto da silvicultura": "produto"}
        grupo_produto = "Silvicultura"
    df = df[list(columns_rename.keys())]
    df = df.rename(columns=columns_rename)
    df = df.assign(
        variavel=lambda x: x["variavel"].replace(
            {
                "Quantidade produzida na extração vegetal": "Quantidade produzida",
                "Valor da produção na extração vegetal": "Valor da produção",
                "Quantidade produzida na silvicultura": "Quantidade produzida",
                "Valor da produção na silvicultura": "Valor da produção",
            }
        ),
        grupo_produto=grupo_produto,
    )
    df = df.astype({"ano": int, "id_municipio": str})
    return df


def main():
    tabelas = get_tabelas()
    data_files = download(tabelas=tabelas)

    db_table = "pevs_producao"
    config = Config(db_table=db_table)
    engine = database.get_engine(config)
    create_table(engine, config)

    for data_file in data_files:
        filepath = data_file["filepath"]
        df = storage.read_file(filepath)
        df = refine(df)
        database.load(df, engine=engine, config=config)


if __name__ == "__main__":
    main()
