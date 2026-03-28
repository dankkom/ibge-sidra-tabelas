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

from typing import Any, Iterable

from ibge_sidra_tabelas import sidra
from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class LavourasTemporariasScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        tabelas = (
            {
                "sidra_tabela": "839",
                "territories": {"6": []},
                "variables": ["allxp"],
                "classifications": {"81": ["allxt"]},
            },
            {
                "sidra_tabela": "1000",
                "territories": {"6": []},
                "variables": ["allxp"],
                "classifications": {"81": ["allxt"]},
            },
            {
                "sidra_tabela": "1001",
                "territories": {"6": []},
                "variables": ["allxp"],
                "classifications": {"81": ["allxt"]},
            },
        )
        tabelas_1002 = tuple(
            {
                "sidra_tabela": "1002",
                "territories": {"6": []},
                "variables": [variable],
                "classifications": {"81": ["allxt"]},
            }
            for variable in ("109", "216", "214", "112")
        )
        metadados_1612 = self.fetcher.sidra_client.get_agregado_metadados("1612")
        tabelas_1612 = tuple(
            {
                "sidra_tabela": "1612",
                "territories": {"6": []},
                "variables": ["allxp"],
                "classifications": classificacoes,
            }
            for classificacoes in sidra.unnest_classificacoes(
                metadados_1612.classificacoes
            )
        )
        return tabelas + tabelas_1002 + tabelas_1612


def main():
    config = Config()
    script = LavourasTemporariasScript(config)
    script.run()


if __name__ == "__main__":
    main()
