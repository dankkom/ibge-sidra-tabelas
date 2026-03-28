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

from typing import Any

from ibge_sidra_tabelas import sidra
from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class ProducaoScript(BaseScript):
    def get_tabelas(self) -> list[dict[str, Any]]:
        tabelas = []
        sidra_tabelas_grandes = (
            "74",
            "3940",
        )
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


def main():
    config = Config()
    script = ProducaoScript(config)
    script.run()


if __name__ == "__main__":
    main()
