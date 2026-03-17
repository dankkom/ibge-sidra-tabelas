# Tabela 1686 - INPC - Variação mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de julho/1989 até dezembro/1990)
# Tabela 1690 - INPC - Peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de julho/1989 até dezembro/1990)
# Tabela 22 - INPC - Variação mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de janeiro/1991 até julho/1999)
# Tabela 23 - INPC - Peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de janeiro/1991 até julho/1999)
# Tabela 653 - INPC - Variação mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de agosto/1999 até junho/2006)
# Tabela 654 - INPC - Peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de agosto/1999 até junho/2006)
# Tabela 2951 - INPC - Variação mensal, acumulada no ano e peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de julho/2006 até dezembro/2011)
# Tabela 1100 - INPC - Variação mensal, acumulada no ano, acumulada em 12 meses e peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de janeiro/2012 até dezembro/2019)
# Tabela 7063 - INPC - Variação mensal, acumulada no ano, acumulada em 12 meses e peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (a partir de janeiro/2020)


from typing import Any

from sqlalchemy import Engine

from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class INPCScript(BaseScript):
    def get_tabelas(self) -> list[dict[str, Any]]:
        tabelas = []
        sidra_tabelas_grandes = (
            "1686",
            "1690",
            "22",
            "23",
            "653",
            "654",
            "2951",
            "1100",
            "7063",
        )
        territories = {"1": ["all"], "6": ["all"], "7": ["all"]}
        variables = ["355", "357"]
        for tabela_grande in sidra_tabelas_grandes:
            if tabela_grande == "7063":
                territories = {
                    "1": ["all"],
                    "6": ["all"],
                    "7": ["all"],
                    "71": ["all"],
                }
            _tabelas = [
                {
                    "sidra_tabela": tabela_grande,
                    "territories": territories,
                    "variables": variables,
                }
            ]
            tabelas.extend(_tabelas)
        return tabelas

    def create_table(self, engine: Engine):
        return super().create_table(engine)

    def refine(self, df):
        return super().refine(df)


def main():
    config = Config(db_table="inpc")
    script = INPCScript(config)
    script.run()


if __name__ == "__main__":
    main()
