import argparse

from sidra_fetcher.agregados import Agregado
from sidra_fetcher.fetcher import SidraClient

from ibge_sidra_tabelas import database, models
from ibge_sidra_tabelas.config import DATA_DIR, Config
from ibge_sidra_tabelas.storage import Storage


def fetch_metadata(table_id: int) -> Agregado:
    with SidraClient() as client:
        agregado: Agregado = client.get_agregado_metadados(int(table_id))
        localidades = []
        for nivel in agregado.nivel_territorial.administrativo:
            localidades.extend(
                client.get_agregado_localidades(
                    agregado_id=int(table_id),
                    localidades_nivel=nivel,
                )
            )
        for nivel in agregado.nivel_territorial.ibge:
            localidades.extend(
                client.get_agregado_localidades(
                    agregado_id=int(table_id),
                    localidades_nivel=nivel,
                )
            )
        for nivel in agregado.nivel_territorial.especial:
            localidades.extend(
                client.get_agregado_localidades(
                    agregado_id=int(table_id),
                    localidades_nivel=nivel,
                )
            )
        agregado.localidades = localidades
        agregado.periodos = client.get_agregado_periodos(int(table_id))

    return agregado


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch metadata of a table from IBGE Sidra"
    )
    parser.add_argument("table", type=str, help="Table ID")
    return parser.parse_args()


def main():
    args = get_args()
    config = Config(db_table="test")
    engine = database.get_engine(config)
    models.Base.metadata.create_all(engine)
    repo = Storage(data_dir=DATA_DIR)
    metadata_filepath = repo.get_metadata_filepath(int(args.table))
    if not metadata_filepath.exists():
        agregado = fetch_metadata(int(args.table))
        repo.write_metadata(agregado)
    else:
        agregado = repo.read_metadata(int(args.table))
    database.save_agregado(engine, agregado)


if __name__ == "__main__":
    main()
