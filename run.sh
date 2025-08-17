#!/usr/bin/env bash
# Small helper to run the SQL generator CLI with environment variables from .env
set -euo pipefail

# If a .env file exists, export its variables into the environment
if [ -f .env ]; then
  # shellcheck disable=SC1091
  set -a
  . .env
  set +a
fi

# Allow overriding the Python executable via $PYTHON (defaults to python3)
exec "uv" "run" src/cli.py "$@"
