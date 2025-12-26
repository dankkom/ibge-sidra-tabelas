"""Produto Interno Bruto dos Municípios

https://sidra.ibge.gov.br/pesquisa/pib-munic/tabelas

Um sistema de indicadores municipais com informações econômicas e sociais é
importante instrumento para o planejamento de políticas públicas. Com a
promulgação da Constituição Federal de 1988, que deu mais responsabilidade e
autonomia aos municípios, ampliaram-se as demandas por informações econômicas
padronizadas e comparáveis em nível municipal, tanto por parte de agentes
públicos e privados, quanto por estudiosos da economia, e pela sociedade em
geral. os resultados do PIB dos Municípios permitem identificar as áreas de
geração de renda, produzindo informações que captam as especifidades do País.

---

Tabela 5938 - Produto interno bruto a preços correntes, impostos, líquidos de
              subsídios, sobre produtos a preços correntes e valor adicionado
              bruto a preços correntes total e por atividade econômica, e
              respectivas participações - Referência 2010

https://sidra.ibge.gov.br/tabela/5938

Notas:

1 - Os dados do último ano disponível estarão sujeitos a revisão quando da
    próxima divulgação.
2 - Os dados da série retropolada (de 2002 a 2009) também têm como referência o
    ano de 2010, seguindo a nova referência das Contas Nacionais.

Fonte: IBGE, em parceria com os Órgãos Estaduais de Estatística, Secretarias
       Estaduais de Governo e Superintendência da Zona Franca de Manaus -
       SUFRAMA

"""

from typing import Any, Iterable

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_sidra_tabelas import database
from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class PibMunicScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        return [
            {
                "sidra_tabela": "5938",
                "territories": {"6": ["all"]},
                "variables": ["37", "498", "513", "517", "525", "543", "6575"],
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
                "variavel": "TEXT",
                "unidade": "TEXT",
                "valor": "BIGINT",
            },
            primary_keys=("ano", "id_municipio", "variavel", "unidade"),
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
        columns = [
            "Ano (Código)",
            "Município (Código)",
            "Variável (Código)",
            "Unidade de Medida (Código)",
            "Valor",
        ]
        df = (
            df.dropna(subset="Valor")[columns]
            .rename(
                columns={
                    "Ano (Código)": "ano",
                    "Município (Código)": "id_municipio",
                    "Variável (Código)": "variavel",
                    "Unidade de Medida (Código)": "unidade",
                    "Valor": "valor",
                },
            )
            .assign(
                id_municipio=lambda x: x["id_municipio"].astype(str),
            )
        )
        return df


def main():
    config = Config(db_table="pibmunic")
    script = PibMunicScript(config)
    script.run()


if __name__ == "__main__":
    main()
