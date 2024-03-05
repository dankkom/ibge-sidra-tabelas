from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_tabelas import database
from ibge_tabelas.config import Config
from ibge_tabelas.sidra import download_table


def read(filepath: Path) -> pd.DataFrame:
    columns = (
        "Ano",
        "Município (Código)",
        "Valor",
    )
    df = pd.read_csv(filepath, skiprows=1, usecols=columns, na_values=["...", "-"])
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
    ddl = f"""
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
    with Session(engine) as session:
        session.execute(sa.text(ddl))
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

    filepaths = []
    for sidra_tabela in sidra_tabelas:
        _filepaths = download_table(
            sidra_tabela=sidra_tabela,
            territorial_level="6",
            ibge_territorial_code="all",
        )
        filepaths.extend(_filepaths)

    for filepath in filepaths:
        df = read(filepath)
        df = refine(df)
        database.load(df, engine, config)


if __name__ == "__main__":
    main()
