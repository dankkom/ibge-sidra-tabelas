"""População brasileira por município segundo os censos do IBGE.

Tabela 200 - População residente, por sexo, situação e grupos de idade -
             Amostra - Características Gerais da População (Vide Notas)

https://sidra.ibge.gov.br/tabela/200

Notas:

1 - Para o ano de 1991, dados do Universo. Para os demais anos, dados da
    Amostra

2 - Até o ano de 1991 os grupos de idade vão até 80 anos ou mais; a partir de
    2000, vão até 100 anos ou mais.

Fonte: IBGE - Censo Demográfico

"""

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
    ddl = database.build_ddl(
        schema=config.db_schema,
        table_name=config.db_table,
        tablespace=config.db_tablespace,
        columns={"ano": "SMALLINT NOT NULL", "id_municipio": "TEXT NOT NULL", "n_pessoas": "INTEGER"},
        primary_keys=("ano", "id_municipio"),
        comment="População por município\nFonte: Censos Demográficos do IBGE",
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
        df = storage.read_file(filepath, usecols=("Ano", "Município (Código)", "Valor"))
        df = refine(df)
        database.load(df, engine, config)


if __name__ == "__main__":
    main()
