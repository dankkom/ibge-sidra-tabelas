"""Pesquisa da Pecuária Municipal - Ovinos tosquiados & Vacas ordenhadas

https://sidra.ibge.gov.br/pesquisa/ppm/tabelas

Objetivo

Fornecer informações estatísticas sobre efetivo dos rebanhos, ovinos
tosquiados, vacas ordenhadas, produtos de origem animal e produção da
aquicultura.

Periodicidade e âmbito de investigação

O inquérito é anual e atinge todo o território nacional, com informações para o
Brasil, Regiões Geográficas, Unidades da Federação, Mesorregiões Geográficas,
Microrregiões Geográficas e Municípios.

---

Tabela 95 - Ovinos tosquiados (Vide Notas)

https://sidra.ibge.gov.br/tabela/95

---

Tabela 94 - Vacas ordenhadas (Vide Notas)

https://sidra.ibge.gov.br/tabela/94

Fonte: IBGE - Pesquisa da Pecuária Municipal

"""

from typing import Any

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_sidra_tabelas import database, sidra, storage
from ibge_sidra_tabelas.config import Config


def get_tabelas() -> list[dict[str, str]]:
    tabelas = [
        {
            "sidra_tabela": "94",
            "territorial_level": "6",
            "ibge_territorial_code": "all",
            "variable": "allxp",
        },
        {
            "sidra_tabela": "95",
            "territorial_level": "6",
            "ibge_territorial_code": "all",
            "variable": "allxp",
        },
    ]
    return tabelas


def download(tabelas: list[dict[str, str]]) -> list[dict[str, Any]]:
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
            "variavel": "TEXT NOT NULL",
            "unidade": "TEXT NOT NULL",
            "valor": "DOUBLE PRECISION",
        },
        primary_keys=("ano", "id_municipio", "variavel"),
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
    df = df[list(columns_rename.keys())]
    df = df.rename(columns=columns_rename)
    df = df.assign(
        valor=lambda x: x["valor"].astype(int),
        variavel=lambda x: x["variavel"].replace(
            {
                "Ovinos tosquiados nos estabelecimentos agropecuários": "Ovinos tosquiados",
            }
        ),
    )
    return df


def main():
    tabelas = get_tabelas()
    data_files = download(tabelas=tabelas)

    db_table = "ppm_exploracao"
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
