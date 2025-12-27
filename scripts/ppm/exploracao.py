from typing import Any, Iterable

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_sidra_tabelas import database
from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class ExploracaoScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        tabelas = [
            {
                "sidra_tabela": "94",
                "territories": {"6": []},
                "variables": ["allxp"],
            },
            {
                "sidra_tabela": "95",
                "territories": {"6": []},
                "variables": ["allxp"],
            },
        ]
        return tabelas

    def create_table(self, engine: sa.Engine):
        ddl = database.build_ddl(
            schema=self.config.db_schema,
            table_name=self.config.db_table,
            tablespace=self.config.db_tablespace,
            columns={
                "ano": "SMALLINT NOT NULL",
                "id_municipio": "TEXT NOT NULL",
                "variavel": "TEXT NOT NULL",
                "unidade": "TEXT NOT NULL",
                "valor": "DOUBLE PRECISION",
            },
            primary_keys=("ano", "id_municipio", "variavel"),
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
        df = df[list(columns_rename.keys())]
        df = df.rename(columns=columns_rename)
        df = df.assign(
            valor=lambda x: x["valor"].astype(int),
            variavel=lambda x: x["variavel"].replace(
                {
                    "Ovinos tosquiados nos estabelecimentos agropecuários": "Ovinos tosquiados",
                }
            ),
        )
        return df


def main():
    config = Config(db_table="ppm_exploracao")
    script = ExploracaoScript(config)
    script.run()


if __name__ == "__main__":
    main()
