"""Pesquisa da Pecuária Municipal - Produção de origem animal & Produção da aquicultura

Tabela 74 - Produção de origem animal, por tipo de produto (Vide Notas)

https://sidra.ibge.gov.br/tabela/74

---

Tabela 3940 - Produção da aquicultura, por tipo de produto (Vide Notas)

https://sidra.ibge.gov.br/tabela/3940

Fonte: IBGE - Pesquisa da Pecuária Municipal

"""

from typing import Any

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_sidra_tabelas import database, sidra, storage
from ibge_sidra_tabelas.config import Config


def get_tabelas(fetcher: sidra.Fetcher) -> list[dict[str, str]]:
    tabelas = []
    sidra_tabelas_grandes = (
        "74",
        "3940",
    )
    for tabela_grande in sidra_tabelas_grandes:
        metadados = fetcher.sidra_client.get_agregado_metadados(tabela_grande)
        _tabelas = [
            {
                "sidra_tabela": tabela_grande,
                "territories": {"6": []},
                "variables": ["allxp"],
                "classifications": classificacoes,
            }
            for classificacoes in sidra.unnest_classificacoes(
                metadados.classificacoes
            )
        ]
        tabelas.extend(_tabelas)
    return tabelas


def download(
    fetcher: sidra.Fetcher, tabelas: list[dict[str, str]]
) -> list[dict[str, Any]]:
    data_files = []
    for tabela in tabelas:
        _filepaths = fetcher.download_table(**tabela)
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
        "Unidade de Medida": "unidade",
        "Valor": "valor",
        "Município (Código)": "id_municipio",
        "Ano": "ano",
        "Variável": "variavel",
    }
    if "Tipo de produto de origem animal" in df.columns:
        grupo_produto = "Pecuária"
        columns_rename |= {"Tipo de produto de origem animal": "produto"}
    elif "Tipo de produto da aquicultura" in df.columns:
        grupo_produto = "Aquicultura"
        columns_rename |= {"Tipo de produto da aquicultura": "produto"}
    df = df[list(columns_rename.keys())]
    df = df.rename(columns=columns_rename)
    df = df.assign(
        grupo_produto=grupo_produto,
        variavel=lambda x: x["variavel"].replace(
            {"Produção de origem animal": "Produção", "Produção da aquicultura": "Produção"}
        ),
    )
    return df


def main():
    with sidra.Fetcher() as fetcher:
        tabelas = get_tabelas(fetcher=fetcher)
        data_files = download(fetcher=fetcher, tabelas=tabelas)

    db_table = "ppm_producao"
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
