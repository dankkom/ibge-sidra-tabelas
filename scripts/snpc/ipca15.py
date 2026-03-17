# Tabela 1646 - IPCA15 - Variação mensal, acumulada no ano e peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de maio/2000 até julho/2006)
# Tabela 1387 - IPCA15 - Variação mensal, acumulada no ano e peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de agosto/2006 até janeiro/2012)
# Tabela 1705 - IPCA15 - Variação mensal, acumulada no ano, acumulada em 12 meses e peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (de fevereiro/2012 até janeiro/2020)
# Tabela 7062 - IPCA15 - Variação mensal, acumulada no ano, acumulada em 12 meses e peso mensal, para o índice geral, grupos, subgrupos, itens e subitens de produtos e serviços (a partir de fevereiro/2020)


from typing import Any

from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class IPCA15Script(BaseScript):
    def get_tabelas(self) -> list[dict[str, Any]]:
        tabelas = []
        sidra_tabelas_grandes = (
            "1646",
            "1387",
            "1705",
            "7062",
        )
        territories = {"1": ["all"], "6": ["all"], "7": ["all"]}
        variables = ["355", "357"]
        for tabela_grande in sidra_tabelas_grandes:
            if tabela_grande == "7062":
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
    script = IPCA15Script(config)
    script.run()


if __name__ == "__main__":
    main()
