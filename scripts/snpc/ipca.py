# Tabela 1692: IPCA - Variação mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de julho/1989 até dezembro/1990)
# Tabela 1693: IPCA - Peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de julho/1989 até dezembro/1990)
# Tabela 58: IPCA - Variação mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de janeiro/1991 até julho/1999)
# Tabela 61: IPCA - Peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de janeiro/1991 até julho/1999)
# Tabela 655: IPCA - Variação mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de agosto/1999 até junho/2006)
# Tabela 656: IPCA - Peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de agosto/1999 até junho/2006)
# Tabela 2938: IPCA - Variação mensal, acumulada no ano e peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de julho/2006 até dezembro/2011)
# Tabela 1419: IPCA - Variação mensal, acumulada no ano, acumulada em 12 meses e peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de janeiro/2012 até dezembro/2019)
# Tabela 7060: IPCA - Variação mensal, acumulada no ano, acumulada em 12 meses e peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (a partir de janeiro/2020)


from typing import Any

from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class IPCAScript(BaseScript):
    def get_tabelas(self) -> list[dict[str, Any]]:
        tabelas = []
        sidra_tabelas_grandes = (
            "1692",
            "1693",
            "58",
            "61",
            "655",
            "656",
            "2938",
            "1419",
            "7060",
        )
        territories = {"1": ["all"], "6": ["all"], "7": ["all"]}
        variables = ["355", "357"]
        for tabela_grande in sidra_tabelas_grandes:
            if tabela_grande == "7060":
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


def main():
    config = Config()
    script = IPCAScript(config)
    script.run()


if __name__ == "__main__":
    main()
