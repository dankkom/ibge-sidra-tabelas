# Tabela 1646 - IPCA15 - Variação mensal, acumulada no ano e peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de maio/2000 até julho/2006)
# Tabela 1387 - IPCA15 - Variação mensal, acumulada no ano e peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de agosto/2006 até janeiro/2012)
# Tabela 1705 - IPCA15 - Variação mensal, acumulada no ano, acumulada em 12 meses e peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de fevereiro/2012 até janeiro/2020)
# Tabela 7062 - IPCA15 - Variação mensal, acumulada no ano, acumulada em 12 meses e peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (a partir de fevereiro/2020)


from typing import Any

from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class IPCA15Script(BaseScript):
    def get_tabelas(self) -> list[dict[str, Any]]:
        tabelas = [
            {
                "sidra_tabela": "1646",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["355", "357"],  # 355: IPCA15 - Variação mensal; 357: IPCA15 - Peso mensal
                "classifications": {"315": []},  # C315: Geral, grupo, subgrupo, item e subitem
            },
            {
                "sidra_tabela": "1387",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["355", "357"],  # 355: IPCA15 - Variação mensal; 357: IPCA15 - Peso mensal
                "classifications": {"315": []},  # C315: Geral, grupo, subgrupo, item e subitem
            },
            {
                "sidra_tabela": "1705",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["355", "357"],  # 355: IPCA15 - Variação mensal; 357: IPCA15 - Peso mensal
                "classifications": {"315": []},  # C315: Geral, grupo, subgrupo, item e subitem
            },
            {
                "sidra_tabela": "7062",
                "territories": {"1": [], "6": [], "7": [], "71": []},
                "variables": ["355", "357"],  # 355: IPCA15 - Variação mensal; 357: IPCA15 - Peso mensal
                "classifications": {"315": []},  # C315: Geral, grupo, subgrupo, item e subitem
            },
        ]
        return tabelas


def main():
    config = Config()
    script = IPCA15Script(config)
    script.run()


if __name__ == "__main__":
    main()
