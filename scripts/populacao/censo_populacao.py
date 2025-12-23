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

from typing import Any, Iterable

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_sidra_tabelas import database
from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class CensoPopulacaoScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        return [
            {
                "sidra_tabela": "200",
                "territories": {"6": ["all"]},  # All municipalities in Brazil
                "variables": ["allxp"],  # All variables
                "classifications": {"2": ["0"], "1": ["0"], "58": ["0"]},
            }
        ]

    def create_table(self, engine: sa.Engine):
        ddl = database.build_ddl(
            schema=self.config.db_schema,
            table_name=self.config.db_table,
            tablespace=self.config.db_tablespace,
            columns={
                "ano": "SMALLINT NOT NULL",
                "id_municipio": "TEXT NOT NULL",
                "n_pessoas": "INTEGER",
            },
            primary_keys=("ano", "id_municipio"),
            comment="População por município\nFonte: Censos Demográficos do IBGE",
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
        df = df.dropna(subset="Valor").rename(
            columns={
                "Ano": "ano",
                "Município (Código)": "id_municipio",
                "Valor": "n_pessoas",
            }
        )
        return df


def main():
    config = Config(db_table="censo_populacao")
    script = CensoPopulacaoScript(config)
    script.run()


if __name__ == "__main__":
    main()
