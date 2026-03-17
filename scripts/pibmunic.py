"""Produto Interno Bruto dos Municípios

https://sidra.ibge.gov.br/pesquisa/pib-munic/tabelas

Um sistema de indicadores municipais com informações econômicas e sociais é
importante instrumento para o planejamento de políticas públicas. Com a
promulgação da Constituição Federal de 1988, que deu mais responsabilidade e
autonomia aos municípios, ampliaram-se as demandas por informações econômicas
padronizadas e comparáveis em nível municipal, tanto por parte de agentes
públicos e privados, quanto por estudiosos da economia, e pela sociedade em
geral. os resultados do PIB dos Municípios permitem identificar as áreas de
geração de renda, produzindo informações que captam as especifidades do País.

---

Tabela 5938 - Produto interno bruto a preços correntes, impostos, líquidos de
              subsídios, sobre produtos a preços correntes e valor adicionado
              bruto a preços correntes total e por atividade econômica, e
              respectivas participações - Referência 2010

https://sidra.ibge.gov.br/tabela/5938

Notas:

1 - Os dados do último ano disponível estarão sujeitos a revisão quando da
    próxima divulgação.
2 - Os dados da série retropolada (de 2002 a 2009) também têm como referência o
    ano de 2010, seguindo a nova referência das Contas Nacionais.

Fonte: IBGE, em parceria com os Órgãos Estaduais de Estatística, Secretarias
       Estaduais de Governo e Superintendência da Zona Franca de Manaus -
       SUFRAMA

"""

from typing import Any, Iterable

from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class PibMunicScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        return [
            {
                "sidra_tabela": "5938",
                "territories": {"6": ["all"]},
                "variables": ["37", "498", "513", "517", "525", "543", "6575"],
            }
        ]


def main():
    config = Config()
    script = PibMunicScript(config)
    script.run()


if __name__ == "__main__":
    main()
