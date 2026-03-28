#!/bin/sh
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

SCRIPT_DIR="${1:-scripts}"

# Check if the scripts directory exists
if [ ! -d "$SCRIPT_DIR" ]; then
  echo "Error: Directory '$SCRIPT_DIR' does not exist."
  exit 1
fi

echo "Starting execution of all scripts in '$SCRIPT_DIR/'..."

# Loop through all .py files in the scripts directory recursively
find "$SCRIPT_DIR" -type f -name "*.py" | sort | while read -r script; do
  echo "========================================"
  echo "Running: $script"
  echo "========================================"

  .venv/bin/python "$script"

  EXIT_CODE=$?
  if [ $EXIT_CODE -ne 0 ]; then
    echo "Warning: '$script' exited with code $EXIT_CODE"
    # Uncomment the next line if you want to stop on the first error:
    # exit $EXIT_CODE
  fi
done

echo "All scripts finished."
