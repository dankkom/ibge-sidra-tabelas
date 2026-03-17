from typing import Any, Iterable

from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class AreaFlorestalScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        return [
            {
                "sidra_tabela": "5930",
                "territories": {"6": []},
                "variables": ["allxp"],
                "classifications": {"734": ["allxt"]},
            }
        ]


def main():
    config = Config()
    script = AreaFlorestalScript(config)
    script.run()


if __name__ == "__main__":
    main()
