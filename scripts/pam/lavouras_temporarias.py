from typing import Any, Iterable

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_sidra_tabelas import database, sidra
from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class LavourasTemporariasScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        tabelas = (
            {
                "sidra_tabela": "839",
                "territories": {"6": []},
                "variables": ["allxp"],
                "classifications": {
                    "81": ["allxt"]
                },  # Produto das lavouras temporárias
            },
            {
                "sidra_tabela": "1000",
                "territories": {"6": []},
                "variables": ["allxp"],
                "classifications": {
                    "81": ["allxt"]
                },  # Produto das lavouras temporárias
            },
            {
                "sidra_tabela": "1001",
                "territories": {"6": []},
                "variables": ["allxp"],
                "classifications": {
                    "81": ["allxt"]
                },  # Produto das lavouras temporárias
            },
        )
        tabelas_1002 = tuple(
            {
                "sidra_tabela": "1002",
                "territories": {"6": []},
                "variables": [variable],
                "classifications": {
                    "81": ["allxt"]
                },  # Produto das lavouras temporárias
            }
            for variable in ("109", "216", "214", "112")
        )
        metadados_1612 = self.fetcher.sidra_client.get_agregado_metadados(
            "1612"
        )
        tabelas_1612 = tuple(
            {
                "sidra_tabela": "1612",
                "territories": {"6": []},
                "variables": ["allxp"],
                "classifications": classificacoes,  # Produto das lavouras temporárias
            }
            for classificacoes in sidra.unnest_classificacoes(
                metadados_1612.classificacoes
            )
        )
        return tabelas + tabelas_1002 + tabelas_1612

    def create_table(self, engine: sa.Engine):
        ddl = database.build_ddl(
            schema=self.config.db_schema,
            table_name=self.config.db_table,
            tablespace=self.config.db_tablespace,
            columns={
                "ano": "SMALLINT NOT NULL",
                "id_municipio": "TEXT NOT NULL",
                "produto": "TEXT NOT NULL",
                "variavel": "TEXT NOT NULL",
                "unidade": "TEXT NOT NULL",
                "valor": "DOUBLE PRECISION",
            },
            primary_keys=("ano", "id_municipio", "produto", "variavel"),
        )
        dcl = database.build_dcl(
            schema=self.config.db_schema,
            table_name=self.config.db_table,
            table_owner=self.config.db_user,
            table_user=self.config.db_readonly_role,
        )
        with Session(engine) as session:
            session.execute(sa.text(ddl))
            session.execute(sa.text(dcl))
            session.commit()

    def refine(self, df: pd.DataFrame) -> pd.DataFrame:
        columns_rename = {
            "Ano (Código)": "ano",
            "Município (Código)": "id_municipio",
            "Produto das lavouras temporárias (Código)": "produto",
            "Variável (Código)": "variavel",
            "Unidade de Medida (Código)": "unidade",
            "Valor": "valor",
        }
        df = df[list(columns_rename.keys())]
        df = df.rename(columns=columns_rename)
        df = df.astype({"ano": int, "id_municipio": str})
        return df


def main():
    config = Config(db_table="pam_lavouras_temporarias")
    script = LavourasTemporariasScript(config)
    script.run()


if __name__ == "__main__":
    main()
