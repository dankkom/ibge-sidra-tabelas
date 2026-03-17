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
