# Copyright (C) 2026 Komesu, D.K. <daniel@dkko.me>
#
# This file is part of ibge-sidra-tabelas.
#
# ibge-sidra-tabelas is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# ibge-sidra-tabelas is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with ibge-sidra-tabelas.  If not, see <https://www.gnu.org/licenses/>.

import argparse

from ibge_sidra_tabelas import database, models
from ibge_sidra_tabelas.config import Config
from ibge_sidra_tabelas.sidra import Fetcher
from ibge_sidra_tabelas.storage import Storage


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch metadata of a table from IBGE Sidra"
    )
    parser.add_argument("table", type=str, help="Table ID")
    return parser.parse_args()


def main():
    args = get_args()
    config = Config()
    engine = database.get_engine(config)
    models.Base.metadata.create_all(engine)

    storage = Storage.default(config)
    metadata_filepath = storage.get_metadata_filepath(int(args.table))

    if not metadata_filepath.exists():
        with Fetcher() as fetcher:
            agregado = fetcher.fetch_metadata(args.table)
        storage.write_metadata(agregado)
    else:
        agregado = storage.read_metadata(int(args.table))

    database.save_agregado(engine, agregado)


if __name__ == "__main__":
    main()
