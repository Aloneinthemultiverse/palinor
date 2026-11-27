#!/usr/bin/env bash
# One-command setup for Linux / macOS: checks prerequisites, installs Python packages into .venv, then starts everything.
#   ./setup.sh
set -e
cd "$(dirname "$0")"

need() { command -v "$1" >/dev/null || { echo "Missing: $1. $2"; exit 1; }; }
need python3 "Install Python 3.11+."
need docker "Install Docker (Docker Desktop, or Docker Engine with the compose plugin)."
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' \
  || { echo "Python 3.11+ is required (found $(python3 -V))."; exit 1; }

[ -d .venv ] || { echo "Creating Python environment (.venv)..."; python3 -m venv .venv; }
echo "Installing Python packages..."
.venv/bin/python -m pip install --quiet --upgrade pip
.venv/bin/python -m pip install --quiet -r requirements.txt

exec ./scripts/start_palinor.sh
