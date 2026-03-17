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
