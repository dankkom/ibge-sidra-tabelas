# Copyright (C) 2026 Komesu, D.K. <daniel@dkko.me>
#
# This file is part of ibge-sidra-tabelas.
#
# ibge-sidra-tabelas is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# ibge-sidra-tabelas is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with ibge-sidra-tabelas.  If not, see <https://www.gnu.org/licenses/>.

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
        tabelas = [
            {
                "sidra_tabela": "1692",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["63"],  # 63: IPCA - Variação mensal
                "classifications": {"72": []},  # C72: Geral, grupos, subgrupos, itens e subitens
            },
            {
                "sidra_tabela": "1693",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["66"],  # 66: IPCA - Peso mensal
                "classifications": {"72": []},  # C72: Geral, grupos, subgrupos, itens e subitens
            },
            {
                "sidra_tabela": "58",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["63"],  # 63: IPCA - Variação mensal
                "classifications": {"72": []},  # C72: Geral, grupos, subgrupos, itens e subitens
            },
            {
                "sidra_tabela": "61",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["66"],  # 66: IPCA - Peso mensal
                "classifications": {"72": []},  # C72: Geral, grupos, subgrupos, itens e subitens
            },
            {
                "sidra_tabela": "655",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["63"],  # 63: IPCA - Variação mensal
                "classifications": {"315": []},  # C315: Geral, grupo, subgrupo, item e subitem
            },
            {
                "sidra_tabela": "656",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["66"],  # 66: IPCA - Peso mensal
                "classifications": {"315": []},  # C315: Geral, grupo, subgrupo, item e subitem
            },
            {
                "sidra_tabela": "2938",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["63", "66"],  # 63: IPCA - Variação mensal; 66: IPCA - Peso mensal
                "classifications": {"315": []},  # C315: Geral, grupo, subgrupo, item e subitem
            },
            {
                "sidra_tabela": "1419",
                "territories": {"1": [], "6": [], "7": []},
                "variables": ["63", "66"],  # 63: IPCA - Variação mensal; 66: IPCA - Peso mensal
                "classifications": {"315": []},  # C315: Geral, grupo, subgrupo, item e subitem
            },
            {
                "sidra_tabela": "7060",
                "territories": {"1": [], "6": [], "7": [], "71": []},
                "variables": ["63", "66"],  # 63: IPCA - Variação mensal; 66: IPCA - Peso mensal
                "classifications": {"315": []},  # C315: Geral, grupo, subgrupo, item e subitem
            },
        ]
        return tabelas


def main():
    config = Config()
    script = IPCAScript(config)
    script.run()


if __name__ == "__main__":
    main()
