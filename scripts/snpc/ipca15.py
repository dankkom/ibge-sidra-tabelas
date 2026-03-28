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
