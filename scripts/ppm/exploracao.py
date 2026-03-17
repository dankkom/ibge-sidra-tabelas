from typing import Any, Iterable

from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class ExploracaoScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        return [
            {
                "sidra_tabela": "94",
                "territories": {"6": []},
                "variables": ["allxp"],
            },
            {
                "sidra_tabela": "95",
                "territories": {"6": []},
                "variables": ["allxp"],
            },
        ]


def main():
    config = Config()
    script = ExploracaoScript(config)
    script.run()


if __name__ == "__main__":
    main()
