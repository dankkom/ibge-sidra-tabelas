from typing import Any

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_sidra_tabelas import database, sidra
from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class ProducaoScript(BaseScript):
    def get_tabelas(self) -> list[dict[str, Any]]:
        tabelas = []
        sidra_tabelas_grandes = (
            "74",
            "3940",
        )
        for tabela_grande in sidra_tabelas_grandes:
            metadados = self.fetcher.sidra_client.get_agregado_metadados(
                tabela_grande
            )
            _tabelas = [
                {
                    "sidra_tabela": tabela_grande,
                    "territories": {"6": []},
                    "variables": ["allxp"],
                    "classifications": classificacoes,
                }
                for classificacoes in sidra.unnest_classificacoes(
                    metadados.classificacoes
                )
            ]
            tabelas.extend(_tabelas)
        return tabelas

    def create_table(self, engine: sa.Engine):
        ddl = database.build_ddl(
            schema=self.config.db_schema,
            table_name=self.config.db_table,
            tablespace=self.config.db_tablespace,
            columns={
                "ano": "SMALLINT NOT NULL",
                "id_municipio": "TEXT NOT NULL",
                "grupo_produto": "TEXT NOT NULL",
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
            "Unidade de Medida (Código)": "unidade",
            "Valor": "valor",
            "Município (Código)": "id_municipio",
            "Ano (Código)": "ano",
            "Variável (Código)": "variavel",
        }
        if "Tipo de produto de origem animal (Código)" in df.columns:
            grupo_produto = "Pecuária"
            columns_rename |= {"Tipo de produto de origem animal (Código)": "produto"}
        elif "Tipo de produto da aquicultura (Código)" in df.columns:
            grupo_produto = "Aquicultura"
            columns_rename |= {"Tipo de produto da aquicultura (Código)": "produto"}
        df = df[list(columns_rename.keys())]
        df = df.rename(columns=columns_rename)
        df = df.assign(
            grupo_produto=grupo_produto,
            variavel=lambda x: x["variavel"].replace(
                {
                    "Produção de origem animal": "Produção",
                    "Produção da aquicultura": "Produção",
                }
            ),
        )
        return df


def main():
    config = Config(db_table="ppm_producao")
    script = ProducaoScript(config)
    script.run()


if __name__ == "__main__":
    main()
