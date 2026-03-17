from typing import Any, Iterable

from ibge_sidra_tabelas import sidra
from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class ProducaoScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        metadados_289 = self.fetcher.sidra_client.get_agregado_metadados("289")
        tabelas_289 = tuple(
            {
                "sidra_tabela": "289",
                "territories": {"6": []},
                "variables": ["allxp"],
                "classifications": classificacoes,
            }
            for classificacoes in sidra.unnest_classificacoes(
                metadados_289.classificacoes
            )
        )
        metadados_291 = self.fetcher.sidra_client.get_agregado_metadados("291")
        tabelas_291 = tuple(
            {
                "sidra_tabela": "291",
                "territories": {"6": []},
                "variables": ["allxp"],
                "classifications": classificacoes,
            }
            for classificacoes in sidra.unnest_classificacoes(
                metadados_291.classificacoes
            )
        )
        return tabelas_289 + tabelas_291


def main():
    config = Config()
    script = ProducaoScript(config)
    script.run()


if __name__ == "__main__":
    main()
