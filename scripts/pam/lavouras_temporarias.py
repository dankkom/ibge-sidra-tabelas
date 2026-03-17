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
        metadados_1612 = self.fetcher.sidra_client.get_agregado_metadados(
            "1612"
        )
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
