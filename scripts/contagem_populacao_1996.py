import pandas as pd
import sidrapy

import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_tabelas.utils import get_engine, get_periodos, temp_dir
from ibge_tabelas.config import Config

sidra_tabela = "305"

db_name = "alpha"
db_schema = "ibge"
db_table = "contagem_populacao"
db_tablespace = "pg_default"

config = Config(db_name)


def download():
    periodos = get_periodos(sidra_tabela)
    for periodo in periodos:
        dest_filepath = temp_dir() / f"{sidra_tabela}-{periodo['id']}.csv"
        if dest_filepath.exists():
            continue
        print(f"Downloading {sidra_tabela}-{periodo['id']}")
        df = sidrapy.get_table(
            table_code=sidra_tabela,  # Tabela SIDRA 305
            territorial_level="6",  # Nível de Municípios
            ibge_territorial_code="all",  # Todos os Municípios
            period=periodo["id"],  # Período
        )
        df.to_csv(dest_filepath, index=False, encoding="utf-8")


def read():
    columns = (
        "Ano",
        "Município (Código)",
        "Valor",
    )
    df = pd.concat(
        (
            pd.read_csv(f, skiprows=1, usecols=columns, na_values=["..."])
            for f in temp_dir().glob(f"{sidra_tabela}-*.csv")
        ),
    )
    return df


def refine(df):
    df = df.dropna(subset="Valor").rename(
        columns={
            "Ano": "ano",
            "Município (Código)": "id_municipio",
            "Valor": "n_pessoas",
        }
    )
    return df


def create_table(engine):
    user = config.db_user
    schema = db_schema
    table_name = db_table
    ddl = sa.text(
        f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name}
    (
        ano smallint NOT NULL,
        id_municipio integer NOT NULL,
        n_pessoas integer,
        CONSTRAINT {table_name}_pkey PRIMARY KEY (ano, id_municipio)
    )

    TABLESPACE {db_tablespace};

    ALTER TABLE IF EXISTS {schema}.{table_name}
        OWNER to {user};
    """
    )
    with Session(engine) as session:
        session.execute(ddl)


def upload(df, engine):
    df.to_sql(
        db_table,
        engine,
        schema=db_schema,
        if_exists="append",
        index=False,
        chunksize=1_000,
    )


def main():
    download()
    df = read()
    df = refine(df)
    engine = get_engine(config, db_name)
    create_table(engine)
    upload(df, engine)


if __name__ == "__main__":
    main()
