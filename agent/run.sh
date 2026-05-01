#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/wanghanming1/quant"
cd "$ROOT"

if [[ -f "$ROOT/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT/.venv/bin/activate"
fi

python -m agent.daily "${1:-papertrade}"
