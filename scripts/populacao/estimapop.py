"""Estimativas de População - EstimaPop

https://sidra.ibge.gov.br/pesquisa/estimapop/tabelas

Apresenta estimativas anuais de população para os municípios e para as Unidades
da Federação brasileiras, com data de referência em 1º de julho.

As estimativas são realizadas para os anos em que não há Censo Demográfico ou
Contagem da População.

---

Tabela 6579 - População residente estimada

https://sidra.ibge.gov.br/tabela/6579

Fonte: IBGE - Estimativas de População

"""

from typing import Any, Iterable

from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class EstimaPopScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        return [
            {
                "sidra_tabela": "6579",
                "territories": {"6": []},
            }
        ]


def main():
    config = Config()
    script = EstimaPopScript(config)
    script.run()


if __name__ == "__main__":
    main()
