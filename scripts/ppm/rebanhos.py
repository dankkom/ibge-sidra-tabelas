from typing import Any, Iterable

from ibge_sidra_tabelas import sidra
from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class RebanhosScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        tabelas = []
        sidra_tabelas_grandes = ("73", "3939")
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
    script = RebanhosScript(config)
    script.run()


if __name__ == "__main__":
    main()
