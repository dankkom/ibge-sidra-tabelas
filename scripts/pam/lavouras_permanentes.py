from typing import Any, Iterable

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_sidra_tabelas import database, sidra
from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class LavourasPermanentesScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        metadados = self.fetcher.sidra_client.get_agregado_metadados("1613")
        tabelas = tuple(
            {
                "sidra_tabela": "1613",
                "territories": {"6": []},  # 6 = Brasil
                "variables": ["allxp"],
                "classifications": classificacoes,  # Produto das lavouras permanentes
            }
            for classificacoes in sidra.unnest_classificacoes(
                metadados.classificacoes
            )
        )
        return tabelas

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
            "Produto das lavouras permanentes (Código)": "produto",
            "Variável (Código)": "variavel",
            "Unidade de Medida (Código)": "unidade",
            "Valor": "valor",
        }
        df = df[list(columns_rename.keys())]
        df = df.rename(columns=columns_rename)
        df = df.astype({"ano": int, "id_municipio": str})
        return df


def main():
    config = Config(db_table="pam_lavouras_permanentes")
    script = LavourasPermanentesScript(config)
    script.run()


if __name__ == "__main__":
    main()
