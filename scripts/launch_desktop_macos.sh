#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CONFIG_PATH="${1:-$ROOT_DIR/configs/mac_desktop_default.yaml}"

if [[ -x "$ROOT_DIR/.venv_gui/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv_gui/bin/python"
elif [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

export PYTHONPATH="$ROOT_DIR/src${PYTHONPATH:+:$PYTHONPATH}"

exec "$PYTHON_BIN" -m fxautotrade_lab.cli launch-desktop --config "$CONFIG_PATH"
