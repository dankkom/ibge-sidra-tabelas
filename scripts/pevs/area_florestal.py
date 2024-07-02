"""Produção da Extração Vegetal e da Silvicultura

Tabela 5930 - Área total existente em 31/12 dos efetivos da silvicultura, por
              espécie florestal (Vide Notas)

https://sidra.ibge.gov.br/tabela/5930

Notas:

1 - Os dados do último ano divulgado são RESULTADOS PRELIMINARES e podem sofrer
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
    tabela_5930 = {
        "sidra_tabela": "5930",
        "territorial_level": "6",
        "ibge_territorial_code": "all",
        "variable": "allxp",
        "classifications": {"734": "allxt"},  # Espécie florestal
    }
    return (tabela_5930,)


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
            "especie_florestal": "TEXT NOT NULL",
            "unidade": "TEXT NOT NULL",
            "area": "INTEGER",
        },
        primary_keys=("ano", "id_municipio", "especie_florestal"),
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
        "Espécie florestal": "especie_florestal",
        "Unidade de Medida": "unidade",
        "Valor": "area",
    }
    df = df[list(columns_rename.keys())]
    df = df.rename(columns=columns_rename)
    df = df.astype({"ano": int, "id_municipio": str})
    return df


def main():
    tabelas = get_tabelas()
    data_files = download(tabelas=tabelas)

    db_table = "pevs_area_florestal"
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
