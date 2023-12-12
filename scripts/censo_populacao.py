"""População brasileira por município segundo os censos do IBGE."""


import pandas as pd
import sidrapy
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_tabelas.config import Config
from ibge_tabelas.sidra import get_periodos
from ibge_tabelas.utils import get_engine, temp_dir


def download(sidra_tabela: str):
    periodos = get_periodos(sidra_tabela)
    for periodo in periodos:
        dest_filepath = temp_dir() / f"{sidra_tabela}-{periodo['id']}.csv"
        if dest_filepath.exists():
            continue
        print(f"Downloading {sidra_tabela}-{periodo['id']}")
        df = sidrapy.get_table(
            table_code=sidra_tabela,  # Tabela SIDRA
            territorial_level="6",  # Nível de Municípios
            ibge_territorial_code="all",  # Todos os Municípios
            period=periodo["id"],  # Período
            variable="allxp",  # Todos exceto total
            classifications={"2": "0", "1": "0", "58": "0"},
        )
        df.to_csv(dest_filepath, index=False, encoding="utf-8")


def read(sidra_tabela: str) -> pd.DataFrame:
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


def refine(df) -> pd.DataFrame:
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
    comment = """População por município
Fonte: Censos Demográficos do IBGE"""
    ddl = f"""
CREATE TABLE IF NOT EXISTS {schema}.{table_name}
(
    ano smallint NOT NULL,
    id_municipio integer NOT NULL,
    n_pessoas integer,
    CONSTRAINT {table_name}_pkey PRIMARY KEY (ano, id_municipio)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS {schema}.{table_name}
    OWNER to {user};

REVOKE ALL ON TABLE {schema}.{table_name} FROM pezco_readonly;

GRANT ALL ON TABLE {schema}.{table_name} TO {user};

GRANT SELECT ON TABLE {schema}.{table_name} TO pezco_readonly;

COMMENT ON TABLE {schema}.{table_name}
    IS '{comment}';
    """
    with Session(engine) as session:
        session.execute(ddl)


def upload(df, engine: sa.engine.Engine, config: Config):
    df.to_sql(
        config.db_table,
        engine,
        schema=config.db_schema,
        if_exists="append",
        index=False,
        chunksize=1_000,
    )


def main():
    sidra_tabela = "200"
    db_table = "censo_populacao"
    config = Config(db_table=db_table)
    download(sidra_tabela)
    df = read(sidra_tabela)
    df = refine(df)
    engine = get_engine(config)
    create_table(engine, config)
    upload(df, engine, config)


if __name__ == "__main__":
    main()
