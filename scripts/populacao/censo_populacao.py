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

"""População brasileira por município segundo os censos do IBGE.

Tabela 200 - População residente, por sexo, situação e grupos de idade -
             Amostra - Características Gerais da População (Vide Notas)

https://sidra.ibge.gov.br/tabela/200

Notas:

1 - Para o ano de 1991, dados do Universo. Para os demais anos, dados da
    Amostra

2 - Até o ano de 1991 os grupos de idade vão até 80 anos ou mais; a partir de
    2000, vão até 100 anos ou mais.

Fonte: IBGE - Censo Demográfico

"""

from typing import Any, Iterable

from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class CensoPopulacaoScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        return [
            {
                "sidra_tabela": "200",
                "territories": {"6": ["all"]},
                "variables": ["allxp"],
                "classifications": {"2": ["0"], "1": ["0"], "58": ["0"]},
            }
        ]


def main():
    config = Config()
    script = CensoPopulacaoScript(config)
    script.run()


if __name__ == "__main__":
    main()
