"""Estimativas de População - EstimaPop

Tabela 6579 - População residente estimada

https://sidra.ibge.gov.br/tabela/6579

"""

import configparser
import os
from pathlib import Path

import pandas as pd
import sidrapy
import sqlalchemy as sa

from utils import get_periodos


tabela = "6579"
temp_dir = Path("./tmp")
temp_dir.mkdir(exist_ok=True)


def download():
    periodos = get_periodos(tabela)
    for periodo in periodos:
        dest_filepath = temp_dir / f"{tabela}-{periodo['id']}.csv"
        if dest_filepath.exists():
            continue
        print(f"Downloading {tabela}-{periodo['id']}")
        df = sidrapy.get_table(
            table_code=tabela,            # Tabela SIDRA 6579
            territorial_level="6",        # Nível de Municípios
            ibge_territorial_code="all",  # Todos os Municípios
            period=periodo["id"],         # Período
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
            for f in temp_dir.glob(f"{tabela}-*.csv")
        ),
    )
    return df


def refine(df):
    df = (
        df.dropna(subset="Valor")
        .rename(
            columns={
                "Ano": "ano",
                "Município (Código)": "id_municipio",
                "Valor": "n_pessoas",
            }
        )
    )
    return df


def upload(df, nome_tabela):
    CONFIG_DIR = Path(os.getenv("CONFIG_DIR"))
    config = configparser.ConfigParser()
    config.read(CONFIG_DIR / "pezcodata-db.ini")
    connection_string = config["DB"]["CONNECTION_STRING_ALPHA"]
    engine = sa.create_engine(connection_string)
    df.to_sql(
        nome_tabela,
        engine,
        schema="ibge",
        if_exists="append",
        index=False,
        chunksize=1_000,
    )


def main():
    download()
    df = read()
    df = refine(df)
    print(df)
    upload(df, "estimapop")


if __name__ == "__main__":
    main()
