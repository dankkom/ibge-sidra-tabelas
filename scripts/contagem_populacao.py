from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_tabelas import database
from ibge_tabelas.config import Config
from ibge_tabelas.sidra import download_table


def read(filepaths: list[Path]) -> pd.DataFrame:
    columns = (
        "Ano",
        "Município (Código)",
        "Valor",
    )
    df = pd.concat(
        (
            pd.read_csv(fp, skiprows=1, usecols=columns, na_values=["...", "-"])
            for fp in filepaths
        ),
    )
    return df


def refine(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset="Valor").rename(
        columns={
            "Ano": "ano",
            "Município (Código)": "id_municipio",
            "Valor": "n_pessoas",
        }
    )
    return df


def create_table(engine: sa.engine.Engine, config: Config):
    user = config.db_user
    schema = config.db_schema
    table_name = config.db_table
    tablespace = config.db_tablespace
    ddl = sa.text(
        f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name}
    (
        ano smallint NOT NULL,
        id_municipio integer NOT NULL,
        n_pessoas integer,
        CONSTRAINT {table_name}_pkey PRIMARY KEY (ano, id_municipio)
    )

    TABLESPACE {tablespace};

    ALTER TABLE IF EXISTS {schema}.{table_name}
        OWNER to {user};
    """
    )
    with Session(engine) as session:
        session.execute(ddl)
        session.commit()


def main():
    sidra_tabelas = (
        "305",
        "793",
    )
    db_table = "contagem_populacao"
    config = Config(db_table=db_table)
    engine = database.get_engine(config)
    create_table(engine, config)

    for sidra_tabela in sidra_tabelas:
        filepaths = download_table(
            sidra_tabela=sidra_tabela,
            territorial_level="6",
            ibge_territorial_code="all",
        )
        df = read(filepaths)
        df = refine(df)
        database.load(df, engine, config)


if __name__ == "__main__":
    main()
