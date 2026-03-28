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

"""CLI entry point for running SQL transformations from TOML files.

Usage::

    python scripts/transform.py transformations/snpc/ipca.toml
"""

import argparse
import logging
from pathlib import Path

from ibge_sidra_tabelas.config import Config
from ibge_sidra_tabelas.transform_runner import TransformRunner


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Run a SQL transformation from a TOML config file",
    )
    parser.add_argument(
        "toml_file",
        type=Path,
        help="Path to the TOML file defining the transformation",
    )
    args = parser.parse_args()

    config = Config()
    runner = TransformRunner(config, args.toml_file)
    runner.run()


if __name__ == "__main__":
    main()
