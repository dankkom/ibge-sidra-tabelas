"""Pesquisa da Pecuária Municipal - Efetivo dos rebanhos, por tipo de rebanho

Tabela 3939 - Efetivo dos rebanhos, por tipo de rebanho (Vide Notas)

https://sidra.ibge.gov.br/tabela/3939

---

Tabela 73 - Efetivo dos rebanhos, por tipo de rebanho (série encerrada) (Vide Notas)

https://sidra.ibge.gov.br/tabela/73

---

Fonte: IBGE - Pesquisa da Pecuária Municipal

"""

from typing import Any

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_sidra_tabelas import database, sidra, storage
from ibge_sidra_tabelas.config import Config


def get_tabelas() -> list[dict[str, str]]:
    tabelas = []
    sidra_tabelas_grandes = ("73", "3939")
    for tabela_grande in sidra_tabelas_grandes:
        metadados = sidra.get_metadados(tabela_grande)
        _tabelas = [
            {
                "sidra_tabela": tabela_grande,
                "territorial_level": "6",
                "ibge_territorial_code": "all",
                "variable": "allxp",
                "classifications": classificacoes,
            }
            for classificacoes in sidra.unnest_classificacoes(metadados["classificacoes"], {})
        ]
        tabelas.extend(_tabelas)
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
            "tipo_rebanho": "TEXT NOT NULL",
            "variavel": "TEXT NOT NULL",
            "unidade": "TEXT NOT NULL",
            "valor": "DOUBLE PRECISION",
        },
        primary_keys=("ano", "id_municipio", "tipo_rebanho", "variavel"),
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
        "Tipo de rebanho": "tipo_rebanho",
    }
    df = df[list(columns_rename.keys())]
    df = df.rename(columns=columns_rename)
    return df


def main():
    tabelas = get_tabelas()
    data_files = download(tabelas=tabelas)

    db_table = "ppm_rebanhos"
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
