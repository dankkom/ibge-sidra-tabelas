"""Produção da Extração Vegetal e da Silvicultura

Tabela 5930 - Área total existente em 31/12 dos efetivos da silvicultura, por
              espécie florestal (Vide Notas)

https://sidra.ibge.gov.br/tabela/5930

Notas:

1 - Os dados do último ano divulgado são RESULTADOS PRELIMINARES e podem sofrer
    alterações até a próxima divulgação.

Fonte: IBGE - Produção da Extração Vegetal e da Silvicultura

"""

from typing import Any, Iterable

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_sidra_tabelas import database
from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class AreaFlorestalScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        tabela_5930 = {
            "sidra_tabela": "5930",
            "territories": {"6": []},  # 6 = Brasil
            "variables": ["allxp"],
            "classifications": {"734": ["allxt"]},  # Espécie florestal
        }
        return (tabela_5930,)

    def create_table(self, engine: sa.Engine):
        ddl = database.build_ddl(
            schema=self.config.db_schema,
            table_name=self.config.db_table,
            tablespace=self.config.db_tablespace,
            columns={
                "ano": "SMALLINT NOT NULL",
                "id_municipio": "TEXT NOT NULL",
                "especie_florestal": "TEXT NOT NULL",
                "unidade": "TEXT NOT NULL",
                "area": "INTEGER",
            },
            primary_keys=("ano", "id_municipio", "especie_florestal"),
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
            "Espécie florestal": "especie_florestal",
            "Unidade de Medida": "unidade",
            "Valor": "area",
        }
        df = df[list(columns_rename.keys())]
        df = df.rename(columns=columns_rename)
        df = df.astype({"ano": int, "id_municipio": str})
        return df


def main():
    config = Config(db_table="pevs_area_florestal")
    script = AreaFlorestalScript(config)
    script.run()


if __name__ == "__main__":
    main()
