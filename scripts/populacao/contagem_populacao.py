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

"""Contagem da População

Tabela 305 - População residente em domicílios particulares permanentes por
             sexo do chefe do domicílio e situação

https://sidra.ibge.gov.br/tabela/305

---

Tabela 793 - População residente (Vide Notas)

https://sidra.ibge.gov.br/tabela/793

Fonte: IBGE - Contagem da População

"""

from typing import Any, Iterable

from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class ContagemPopulacaoScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        return [
            {
                "sidra_tabela": "305",
                "territories": {"6": ["all"]},
                "variables": ["allxp"],
                "classifications": {"293": ["0"], "1": ["0"]},
            },
            {
                "sidra_tabela": "793",
                "territories": {"6": ["all"]},
            },
        ]


def main():
    config = Config()
    script = ContagemPopulacaoScript(config)
    script.run()


if __name__ == "__main__":
    main()
