from typing import Any, Iterable

from ibge_sidra_tabelas import sidra
from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class LavourasPermanentesScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        metadados = self.fetcher.sidra_client.get_agregado_metadados("1613")
        tabelas = tuple(
            {
                "sidra_tabela": "1613",
                "territories": {"6": []},
                "variables": ["allxp"],
                "classifications": classificacoes,
            }
            for classificacoes in sidra.unnest_classificacoes(
                metadados.classificacoes
            )
        )
        return tabelas


def main():
    config = Config()
    script = LavourasPermanentesScript(config)
    script.run()


if __name__ == "__main__":
    main()
