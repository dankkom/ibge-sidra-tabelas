"""Produção da Extração Vegetal e da Silvicultura

Tabela 289 - Quantidade produzida e valor da produção na extração vegetal, por
             tipo de produto extrativo (Vide Notas)

https://sidra.ibge.gov.br/tabela/289

Notas:

1 - Os municípios sem informação para pelo menos um produto da extração vegetal
    não aparecem nas listas.

2 - Até 2001 era pesquisada a erva-mate cancheada. A partir de 2002 passou-se a
    pesquisar a erva-mate folha verde.

3 - Valor da produção na extração vegetal: Variável derivada calculada pela
    média ponderada das informações de quantidade e preço médio corrente pago
    ao produtor, de acordo com os períodos de colheita e comercialização de
    cada produto. As despesas de frete, taxas e impostos não são incluídas no
    preço.

4 - Os dados do último ano divulgado são RESULTADOS PRELIMINARES e podem sofrer
    alterações até a próxima divulgação.

---

Tabela 291 - Quantidade produzida e valor da produção na silvicultura, por tipo
             de produto da silvicultura (Vide Notas)

https://sidra.ibge.gov.br/tabela/291

Notas:

1 - As Unidades da Federação, mesorregiões, microrregiões e municípios sem
    informação para pelo menos um produto da silvicultura em pelo menos um ano
    da pesquisa não aparecem nas listas.

2 - Valor da produção na silvicultura: Variável derivada calculada pela média
    ponderada das informações de quantidade e preço médio corrente pago ao
    produtor, de acordo com os períodos de colheita e comercialização de cada
    produto. As despesas de frete, taxas e impostos não são incluídas no preço.

3 - Os dados do último ano divulgado são RESULTADOS PRELIMINARES e podem sofrer
    alterações até a próxima divulgação.

Fonte: IBGE - Produção da Extração Vegetal e da Silvicultura

"""

from typing import Any, Iterable

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_sidra_tabelas import database, sidra
from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class ProducaoScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        metadados_289 = self.fetcher.sidra_client.get_agregado_metadados("289")
        tabelas_289 = tuple(
            {
                "sidra_tabela": "289",
                "territories": {"6": []},
                "variables": ["allxp"],
                "classifications": classificacoes,  # Tipo de produto extrativo
            }
            for classificacoes in sidra.unnest_classificacoes(
                metadados_289.classificacoes
            )
        )
        metadados_291 = self.fetcher.sidra_client.get_agregado_metadados("291")
        tabelas_291 = tuple(
            {
                "sidra_tabela": "291",
                "territories": {"6": []},
                "variables": ["allxp"],
                "classifications": classificacoes,  # Tipo de produto da silvicultura
            }
            for classificacoes in sidra.unnest_classificacoes(
                metadados_291.classificacoes
            )
        )
        return tabelas_289 + tabelas_291

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
            "Ano (Código)": "ano",
            "Município (Código)": "id_municipio",
            "Variável (Código)": "variavel",
            "Unidade de Medida (Código)": "unidade",
            "Valor": "valor",
        }
        if "Tipo de produto extrativo (Código)" in df.columns:
            columns_rename |= {"Tipo de produto extrativo (Código)": "produto"}
            grupo_produto = "Extração vegetal"
        elif "Tipo de produto da silvicultura (Código)" in df.columns:
            columns_rename |= {"Tipo de produto da silvicultura (Código)": "produto"}
            grupo_produto = "Silvicultura"
        df = df[list(columns_rename.keys())]
        df = df.rename(columns=columns_rename)
        df = df.assign(
            variavel=lambda x: x["variavel"].replace(
                {
                    "Quantidade produzida na extração vegetal": "Quantidade produzida",
                    "Valor da produção na extração vegetal": "Valor da produção",
                    "Quantidade produzida na silvicultura": "Quantidade produzida",
                    "Valor da produção na silvicultura": "Valor da produção",
                }
            ),
            grupo_produto=grupo_produto,
        )
        df = df.astype({"ano": int, "id_municipio": str})
        return df


def main():
    config = Config(db_table="pevs_producao")
    script = ProducaoScript(config)
    script.run()


if __name__ == "__main__":
    main()
