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

from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class INPCScript(BaseScript):
    def get_tabelas(self) -> list[dict[str, Any]]:
        tabelas = [
            {
                "sidra_tabela": "1686",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["44"],  # 44: INPC - Variação mensal
                "classifications": {"72": []},  # C72: Geral, grupos, subgrupos, itens e subitens
            },
            {
                "sidra_tabela": "1690",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["45"],  # 45: INPC - Peso mensal
                "classifications": {"72": []},  # C72: Geral, grupos, subgrupos, itens e subitens
            },
            {
                "sidra_tabela": "22",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["44"],  # 44: INPC - Variação mensal
                "classifications": {"72": []},  # C72: Geral, grupos, subgrupos, itens e subitens
            },
            {
                "sidra_tabela": "23",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["45"],  # 45: INPC - Peso mensal
                "classifications": {"72": []},  # C72: Geral, grupos, subgrupos, itens e subitens
            },
            {
                "sidra_tabela": "653",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["44"],  # 44: INPC - Variação mensal
                "classifications": {"315": []},  # C315: Geral, grupo, subgrupo, item e subitem
            },
            {
                "sidra_tabela": "654",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["45"],  # 45: INPC - Peso mensal
                "classifications": {"315": []},  # C315: Geral, grupo, subgrupo, item e subitem
            },
            {
                "sidra_tabela": "2951",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["44", "45"],  # 44: INPC - Variação mensal; 45: INPC - Peso mensal
                "classifications": {"315": []},  # C315: Geral, grupo, subgrupo, item e subitem
            },
            {
                "sidra_tabela": "1100",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["44", "45"],  # 44: INPC - Variação mensal; 45: INPC - Peso mensal
                "classifications": {"315": []},  # C315: Geral, grupo, subgrupo, item e subitem
            },
            {
                "sidra_tabela": "7063",
                "territories": {"1": [], "6": [], "7": [], "71": []},
                "variables": ["44", "45"],  # 44: INPC - Variação mensal; 45: INPC - Peso mensal
                "classifications": {"315": []},  # C315: Geral, grupo, subgrupo, item e subitem
            },
        ]
        return tabelas


def main():
    config = Config()
    script = INPCScript(config)
    script.run()


if __name__ == "__main__":
    main()
