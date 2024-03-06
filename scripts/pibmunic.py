"""Produto Interno Bruto dos Municípios

Tabela 5938 - Produto interno bruto a preços correntes, impostos, líquidos de
subsídios, sobre produtos a preços correntes e valor adicionado bruto a preços
correntes total e por atividade econômica, e respectivas participações -
Referência 2010

https://sidra.ibge.gov.br/tabela/5938

Notas:

1 - Os dados do último ano disponível estarão sujeitos a revisão quando da
    próxima divulgação.
2 - Os dados da série retropolada (de 2002 a 2009) também têm como referência o
    ano de 2010, seguindo a nova referência das Contas Nacionais.

Fonte: IBGE, em parceria com os Órgãos Estaduais de Estatística, Secretarias
Estaduais de Governo e Superintendência da Zona Franca de Manaus - SUFRAMA

"""

from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_tabelas import database, sidra
from ibge_tabelas.config import Config


def read(filepath: Path) -> pd.DataFrame:
    columns = (
        "Ano",
        "Município (Código)",
        "Variável",
        "Unidade de Medida",
        "Valor",
    )
    df = pd.read_csv(filepath, skiprows=1, usecols=columns, na_values=["...", "-"])
    return df


def refine(df: pd.DataFrame) -> pd.DataFrame:
    columns = ["Ano", "Município (Código)", "Variável", "Unidade de Medida", "Valor"]
    df = (
        df.dropna(subset="Valor")[columns]
        .rename(
            columns={
                "Ano": "ano",
                "Município (Código)": "id_municipio",
                "Variável": "variavel",
                "Unidade de Medida": "unidade",
                "Valor": "valor",
            },
        )
        .assign(
            id_municipio=lambda x: x["id_municipio"].astype(str),
        )
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
        ano SMALLINT NOT NULL,
        id_municipio TEXT NOT NULL,
        variavel TEXT,
        unidade TEXT,
        valor BIGINT,
        CONSTRAINT {table_name}_pkey PRIMARY KEY (ano, id_municipio, variavel, unidade)
    )

    TABLESPACE {tablespace};

    ALTER TABLE IF EXISTS {schema}.{table_name}
        OWNER to {user};
    """
    with Session(engine) as session:
        session.execute(sa.text(ddl))
        session.commit()


def main():
    sidra_tabela = "5938"

    filepaths = sidra.download_table(
        sidra_tabela=sidra_tabela,
        territorial_level="6",
        ibge_territorial_code="all",
        variable="37,498,513,517,525,543,6575",
    )

    db_table = "pibmunic"
    config = Config(db_table)
    engine = database.get_engine(config)
    create_table(engine, config)
    for filepath in filepaths:
        df = read(filepath)

        df = refine(df)

        database.load(df, engine, config)


if __name__ == "__main__":
    main()
