"""População brasileira por município segundo os censos do IBGE."""

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_tabelas import database, sidra, storage
from ibge_tabelas.config import Config


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
        session.execute(sa.text(ddl))
        session.commit()


def main():
    sidra_tabela = "200"
    db_table = "censo_populacao"
    config = Config(db_table=db_table)
    engine = database.get_engine(config)
    create_table(engine, config)

    filepaths = sidra.download_table(
        sidra_tabela=sidra_tabela,
        territorial_level="6",
        ibge_territorial_code="all",
        variable="allxp",
        classifications={"2": "0", "1": "0", "58": "0"},
    )

    for filepath in filepaths:
        df = storage.read_file(filepath, columns=("Ano", "Município (Código)", "Valor"))
        df = refine(df)
        database.load(df, engine, config)


if __name__ == "__main__":
    main()
