"""Pesquisa da Pecuária Municipal - Efetivo dos rebanhos, por tipo de rebanho

Tabela 3939 - Efetivo dos rebanhos, por tipo de rebanho (Vide Notas)

https://sidra.ibge.gov.br/tabela/3939

---

Tabela 73 - Efetivo dos rebanhos, por tipo de rebanho (série encerrada) (Vide Notas)

https://sidra.ibge.gov.br/tabela/73

---

Fonte: IBGE - Pesquisa da Pecuária Municipal

"""

from typing import Any, Iterable

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_sidra_tabelas import database, sidra
from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class RebanhosScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        tabelas = []
        sidra_tabelas_grandes = ("73", "3939")
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
                "tipo_rebanho": "TEXT NOT NULL",
                "variavel": "TEXT NOT NULL",
                "unidade": "TEXT NOT NULL",
                "valor": "DOUBLE PRECISION",
            },
            primary_keys=("ano", "id_municipio", "tipo_rebanho", "variavel"),
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
            "Tipo de rebanho (Código)": "tipo_rebanho",
        }
        df = df[list(columns_rename.keys())]
        df = df.rename(columns=columns_rename)
        df = df.dropna(subset=["valor"]).drop_duplicates()
        return df


def main():
    config = Config(db_table="ppm_rebanhos")
    script = RebanhosScript(config)
    script.run()


if __name__ == "__main__":
    main()
