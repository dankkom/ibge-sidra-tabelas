#!/bin/sh

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
