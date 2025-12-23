"""Contagem da População

Tabela 305 - População residente em domicílios particulares permanentes por
             sexo do chefe do domicílio e situação

https://sidra.ibge.gov.br/tabela/305

---

Tabela 793 - População residente (Vide Notas)

https://sidra.ibge.gov.br/tabela/793

Notas:

1 - Inclusive a população estimada nos domicílios fechados.

2 - Inclusive a população estimada nos domicílios provenientes de 10 setores
    censitários cujos arquivos foram danificados: Tocantins, Alagoas, Bahia,
    São Paulo e Paraná.

3 - Somente participaram da pesquisa os municípios com até 160.000 habitantes,
    por isto, os seguintes municípios tiveram a sua população estimada:

    Ananindeua - PA,
    Belém - PA,
    Marabá - PA,
    Santarém - PA,
    Caucaia - CE,
    Fortaleza - CE,
    Juazeiro do Norte - CE,
    Maracanaú - CE,
    Sobral - CE,
    Caruaru - PE,
    Jaboatão dos Guararapes - PE,
    Olinda - PE,
    Paulista - PE,
    Petrolina - PE,
    Recife - PE,
    Camaçari - BA,
    Feira de Santana - BA,
    Ilhéus - BA,
    Itabuna - BA,
    Juazeiro - BA,
    Salvador - BA,
    Vitória da Conquista - BA,
    Belo Horizonte - MG,
    Betim - MG,
    Contagem - MG,
    Divinópolis - MG,
    Governador Valadares - MG,
    Ipatinga - MG,
    Juiz de Fora - MG,
    Montes Claros - MG,
    Ribeirão das Neves - MG,
    Santa Luzia - MG,
    Sete Lagoas - MG,
    Uberaba - MG,
    Uberlândia - MG,
    Cachoeiro de Itapemirim - ES,
    Cariacica - ES,
    Serra - ES,
    Vila Velha - ES,
    Vitória - ES,
    Barra Mansa - RJ,
    Belford Roxo - RJ,
    Campos dos Goytacazes - RJ,
    Duque de Caxias - RJ,
    Itaboraí - RJ,
    Magé - RJ,
    Mesquita - RJ,
    Niterói - RJ,
    Nova Friburgo - RJ,
    Nova Iguaçu - RJ,
    Petrópolis - RJ,
    Rio de Janeiro - RJ,
    São Gonçalo - RJ,
    São João de Meriti - RJ,
    Volta Redonda - RJ,
    Americana - SP,
    Araçatuba - SP,
    Araraquara - SP,
    Barueri - SP,
    Bauru - SP,
    Campinas - SP,
    Carapicuíba - SP,
    Cotia - SP,
    Diadema - SP,
    Embu - SP,
    Ferraz de Vasconcelos - SP,
    Franca - SP,
    Guarujá - SP,
    Guarulhos - SP,
    Hortolândia - SP,
    Indaiatuba - SP,
    Itapevi - SP,
    Itaquaquecetuba - SP,
    Jacareí - SP,
    Jundiaí - SP,
    Limeira - SP,
    Marília - SP,
    Mauá - SP,
    Mogi das Cruzes - SP,
    Osasco - SP,
    Piracicaba - SP,
    Praia Grande - SP,
    Presidente Prudente - SP,
    Ribeirão Preto - SP,
    Rio Claro - SP,
    Santa Bárbara d'Oeste - SP,
    Santo André - SP,
    Santos - SP,
    São Bernardo do Campo - SP,
    São Carlos - SP,
    São José do Rio Preto - SP,
    São José dos Campos - SP,
    São Paulo - SP,
    São Vicente - SP,
    Sorocaba - SP,
    Sumaré - SP,
    Suzano - SP,
    Taboão da Serra - SP,
    Taubaté - SP,
    Cascavel - PR,
    Colombo - PR,
    Curitiba - PR,
    Foz do Iguaçu - PR,
    Londrina - PR,
    Maringá - PR,
    Ponta Grossa - PR,
    São José dos Pinhais - PR,
    Blumenau - SC,
    Criciúma - SC,
    Florianópolis - SC,
    Joinville - SC,
    São José - SC,
    Alvorada - RS,
    Canoas - RS,
    Caxias do Sul - RS,
    Gravataí - RS,
    Novo Hamburgo - RS,
    Passo Fundo - RS,
    Pelotas - RS,
    Porto Alegre - RS,
    Rio Grande - RS,
    Santa Maria - RS,
    São Leopoldo - RS,
    Viamão - RS,
    Anápolis - GO,
    Aparecida de Goiânia - GO,
    Goiânia - GO,
    Luziânia - GO e
    Brasília - DF.

Fonte: IBGE - Contagem da População

"""

from typing import Any, Iterable

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_sidra_tabelas import database
from ibge_sidra_tabelas.base import BaseScript
from ibge_sidra_tabelas.config import Config


class ContagemPopulacaoScript(BaseScript):
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        sidra_tabelas = (
            "305",
            "793",
        )
        return [
            {
                "sidra_tabela": sidra_tabela,
                "territories": {"6": ["all"]},  # All municipalities in Brazil
            }
            for sidra_tabela in sidra_tabelas
        ]

    def create_table(self, engine: sa.Engine):
        ddl = database.build_ddl(
            schema=self.config.db_schema,
            table_name=self.config.db_table,
            tablespace=self.config.db_tablespace,
            columns={
                "ano": "SMALLINT NOT NULL",
                "id_municipio": "TEXT NOT NULL",
                "n_pessoas": "INTEGER",
            },
            primary_keys=("ano", "id_municipio"),
            comment="População por município\nFonte: Contagem da População do IBGE",
        )
        dcl = database.build_dcl(
            schema=self.config.db_schema,
            table_name=self.config.db_table,
            table_owner=self.config.db_user,
            table_user=self.config.db_readonly_role,
        )
        with Session(engine) as session:
            session.execute(sa.text(ddl))
            session.execute(sa.text(dcl))
            session.commit()

    def refine(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(subset="Valor").rename(
            columns={
                "Ano": "ano",
                "Município (Código)": "id_municipio",
                "Valor": "n_pessoas",
            }
        )
        return df


def main():
    config = Config(db_table="contagem_populacao")
    script = ContagemPopulacaoScript(config)
    script.run()


if __name__ == "__main__":
    main()
