"""Produção Agrícola Municipal

Tabela 1613 - Área destinada à colheita, área colhida, quantidade produzida,
              rendimento médio e valor da produção das lavouras permanentes
              (Vide Notas)

https://sidra.ibge.gov.br/tabela/1613

Notas:

1 - Os municípios sem informação para pelo menos um produto da lavoura
    permanente não aparecem nas listas.

2 - A partir do ano de 2001 as quantidades produzidas dos produtos abacate,
    banana, caqui, figo, goiaba, laranja, limão, maçã, mamão, manga, maracujá,
    marmelo, pera, pêssego e tangerina passam a ser expressas em toneladas.
    Nos anos anteriores eram expressas em mil frutos, com exceção da banana,
    que era expressa em mil cachos. O rendimento médio passa a ser expresso em
    Kg/ha. Nos anos anteriores era expresso em frutos/ha, com exceção da
    banana, que era expressa em cachos/ha.

3 - Veja em o documento AlteracoesUnidadesMedidaFrutas.pdf com as alterações de
    unidades de medida das frutíferas ocorridas em 2001 e a tabela de conversão
    fruto x quilograma.

4 - Até 2001, café (em coco), a partir de 2002, café (beneficiado ou em grão).

5 - A quantidade produzida de coco-da-baía é expressa em mil frutos e o
    rendimento médio em frutos/ha.

6 - Valores para a categoria Total indisponíveis para a variável Quantidade
    produzida e Rendimento médio, pois as unidades de medida diferem para
    determinados produtos.

7 - Subentende a possibilidade de cultivos sucessivos ou simultâneos (simples,
    associados e/ou intercalados) no mesmo ano e no mesmo local, podendo, por
    isto, a área informada da cultura exceder a área geográfica do município.

8 - A diferença entre a área destinada à colheita e a área colhida na lavoura
    permanente é considerada como área perdida.

9 - A variável Área destinada à colheita só passou a ser informada a partir de
    1988.

10 - Valor da produção: Variável derivada calculada pela média ponderada das
     informações de quantidade e preço médio corrente pago ao produtor, de
     acordo com os períodos de colheita e comercialização de cada produto. As
     despesas de frete, taxas e impostos não são incluídas no preço.

11 - Os dados do último ano divulgado são RESULTADOS PRELIMINARES e podem
     sofrer alterações até a próxima divulgação.

Fonte: IBGE - Produção Agrícola Municipal

"""

from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.orm import Session

from ibge_tabelas import database, sidra, storage
from ibge_tabelas.config import Config


def get_tabelas():
    metadados = sidra.get_metadados("1613")
    tabelas = tuple(
        {
            "sidra_tabela": "1613",
            "territorial_level": "6",
            "ibge_territorial_code": "all",
            "variable": "allxp",
            "classifications": classificacoes,  # Produto das lavouras permanentes
        }
        for classificacoes in sidra.unnest_classificacoes(metadados["classificacoes"], {})
    )
    return tabelas


def download(tabelas: list[dict[str, str]]) -> list[Path]:
    filepaths = []
    for tabela in tabelas:
        _filepaths = sidra.download_table(**tabela)
        filepaths.extend(_filepaths)
    return filepaths


def create_table(engine: sa.engine.Engine, config: Config):
    ddl = database.build_ddl(
        schema=config.db_schema,
        table_name=config.db_table,
        tablespace=config.db_tablespace,
        columns={
            "ano": "SMALLINT NOT NULL",
            "id_municipio": "TEXT NOT NULL",
            "produto": "TEXT NOT NULL",
            "variavel": "TEXT NOT NULL",
            "unidade": "TEXT NOT NULL",
            "valor": "DOUBLE PRECISION",
        },
        primary_keys=("ano", "id_municipio", "produto", "variavel"),
    )
    dcl = database.build_dcl(
        schema=config.db_schema,
        table_name=config.db_table,
        table_owner=config.db_user,
        table_user=config.db_readonly_role,
    )
    with Session(engine) as session:
        session.execute(sa.text(ddl))
        session.execute(sa.text(dcl))
        session.commit()


def refine(df):
    columns_rename = {
        "Ano (Código)": "ano",
        "Município (Código)": "id_municipio",
        "Produto das lavouras permanentes": "produto",
        "Variável": "variavel",
        "Unidade de Medida": "unidade",
        "Valor": "valor",
    }
    df = df[list(columns_rename.keys())]
    df = df.rename(columns=columns_rename)
    df = df.astype({"ano": int, "id_municipio": str})
    return df


def main():
    tabelas = get_tabelas()
    filepaths = download(tabelas=tabelas)

    db_table = "pam_lavouras_permanentes"
    config = Config(db_table)
    engine = database.get_engine(config)
    create_table(engine, config)

    for filepath in filepaths:
        df = storage.read_file(filepath)
        df = refine(df)
        database.load(df, engine=engine, config=config)


if __name__ == "__main__":
    main()
