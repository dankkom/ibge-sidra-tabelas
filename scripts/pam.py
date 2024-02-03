"""Produção Agrícola Municipal"""

from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.orm import Session
import pandas as pd

from ibge_tabelas.config import Config
from ibge_tabelas import database, sidra, utils


def download():
    tabelas = (
        {
            "sidra_tabela": "839",
            "territorial_level": "6",
            "ibge_territorial_code": "all",
            "variable": "allxp",
            "classifications": {"81": "allxt"},  # Produto das lavouras temporárias
        },
        {
            "sidra_tabela": "1000",
            "territorial_level": "6",
            "ibge_territorial_code": "all",
            "variable": "allxp",
            "classifications": {"81": "allxt"},  # Produto das lavouras temporárias
        },
        {
            "sidra_tabela": "1001",
            "territorial_level": "6",
            "ibge_territorial_code": "all",
            "variable": "allxp",
            "classifications": {"81": "allxt"},  # Produto das lavouras temporárias
        },
    )
    tabelas_1002 = tuple(
        {
            "sidra_tabela": "1002",
            "territorial_level": "6",
            "ibge_territorial_code": "all",
            "variable": variable,
            "classifications": {"81": "allxt"},  # Produto das lavouras temporárias
        }
        for variable in ("109", "216", "214", "112")
    )
    metadados_1612 = sidra.get_metadados("1612")
    tabelas_1612 = tuple(
        {
            "sidra_tabela": "1612",
            "territorial_level": "6",
            "ibge_territorial_code": "all",
            "variable": "allxp",
            "classifications": classificacoes,  # Produto das lavouras temporárias
        }
        for classificacoes in utils.unnest_classificacoes(
            metadados_1612["classificacoes"], {}
        )
    )
    metadados_1613 = sidra.get_metadados("1613")
    tabelas_1613 = tuple(
        {
            "sidra_tabela": "1613",
            "territorial_level": "6",
            "ibge_territorial_code": "all",
            "variable": "allxp",
            "classifications": classificacoes,  # Produto das lavouras permanentes
        }
        for classificacoes in utils.unnest_classificacoes(
            metadados_1613["classificacoes"], {}
        )
    )
    tabelas = tabelas + tabelas_1002 + tabelas_1612 + tabelas_1613
    for tabela in tabelas:
        sidra.download_table(**tabela)


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
        produto TEXT NOT NULL,
        tipo_cultura TEXT NOT NULL,
        variavel TEXT NOT NULL,
        unidade TEXT NOT NULL,
        valor DOUBLE PRECISION,
        CONSTRAINT {table_name}_pkey PRIMARY KEY (ano, id_municipio, produto, variavel)
    )

    TABLESPACE {tablespace};

    ALTER TABLE IF EXISTS {schema}.{table_name}
        OWNER to {user};
    """
    with Session(engine) as session:
        session.execute(sa.text(ddl))
        session.commit()


def read_file(filepath: Path) -> pd.DataFrame:
    print("Reading", filepath)
    return pd.read_csv(filepath, skiprows=1, na_values=["...", "-"])


def read_temp_crops():
    tabelas = (
        "839",
        "1000",
        "1001",
        "1002",
        "1612",
    )
    df = pd.DataFrame()
    for tabela in tabelas:
        data_dir = utils.get_data_dir() / f"t-{tabela}"
        print(data_dir)
        _df = pd.concat(
            (
                read_file(f)
                for f in data_dir.glob(f"t-{tabela}_*.csv")
            ),
            ignore_index=True,
        )
        df = pd.concat((df, _df), ignore_index=True)
    df = (
        df.rename(
            columns={
                "Produto das lavouras temporárias (Código)": "codigo_produto",
                "Produto das lavouras temporárias": "produto",
            },
        )
        .assign(tipo_cultura="Lavouras temporárias")
    )
    return df


def read_perm_crops():
    tabelas = (
        "1613",
    )
    df = pd.DataFrame()
    for tabela in tabelas:
        data_dir = utils.get_data_dir() / f"t-{tabela}"
        print(data_dir)
        _df = pd.concat(
            (
                read_file(f)
                for f in data_dir.glob(f"t-{tabela}_*.csv")
            ),
            ignore_index=True,
        )
        df = pd.concat((df, _df), ignore_index=True)
    df = (
        df.rename(
            columns={
                "Produto das lavouras permanentes (Código)": "codigo_produto",
                "Produto das lavouras permanentes": "produto",
            },
        )
        .assign(tipo_cultura="Lavouras permanentes")
    )
    return df


def refine_temp_crops(df):
    return (
        df.rename(
            columns={
                "Produto das lavouras temporárias (Código)": "codigo_produto",
                "Produto das lavouras temporárias": "produto",
            },
        )
        .assign(tipo_cultura="Lavouras temporárias")
    )


def refine_perm_crops(df):
    return (
        df.rename(
            columns={
                "Produto das lavouras permanentes (Código)": "codigo_produto",
                "Produto das lavouras permanentes": "produto",
            },
        )
        .assign(tipo_cultura="Lavouras permanentes")
    )


def refine(df, tipo_cultura):
    df = df.dropna(subset="Valor")
    df = df.rename(
        columns={
            "Ano (Código)": "ano",
            "Município (Código)": "id_municipio",
            "Variável": "variavel",
            "Unidade de Medida": "unidade",
            "Valor": "valor",
        },
    )
    df = df.dropna()
    df = df.astype({"ano": int, "id_municipio": int})
    match tipo_cultura:
        case "Lavouras temporárias":
            df = refine_temp_crops(df)
        case "Lavouras permanentes":
            df = refine_perm_crops(df)
    df = df[
        [
            "ano",
            "id_municipio",
            "produto",
            "tipo_cultura",
            "variavel",
            "unidade",
            "valor",
        ]
    ]
    return df


def main():
    # download()

    db_table = "producao_agricola_municipal"
    config = Config(db_table)
    engine = database.get_engine(config)
    create_table(engine, config)

    tabelas_temp_crops = (
        "839",
        "1000",
        "1001",
        "1002",
        "1612",
    )
    for tabela in tabelas_temp_crops:
        data_dir = utils.get_data_dir() / f"t-{tabela}"
        for f in data_dir.glob(f"t-{tabela}_*.csv"):
            print(f)
            _df = read_file(f)
            _df = refine(_df, tipo_cultura="Lavouras temporárias")
            database.load(_df, engine, config)

    tabelas_perm_crops = (
        "1613",
    )
    for tabela in tabelas_perm_crops:
        data_dir = utils.get_data_dir() / f"t-{tabela}"
        for f in data_dir.glob(f"t-{tabela}_*.csv"):
            print(f)
            _df = read_file(f)
            _df = refine(_df, tipo_cultura="Lavouras permanentes")
            database.load(_df, engine, config)


if __name__ == "__main__":
    main()
