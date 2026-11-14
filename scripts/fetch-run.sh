#!/bin/bash
# On your machine: copy a run from the two servers into results/<run>/ and summarise it.
#
#   scripts/fetch-run.sh <run>
#
# The SSH hosts of the two servers (as you log in to them, jump hosts in ~/.ssh/config)
# come from scripts/fetch.env, not tracked:
#   cp scripts/fetch.env.example scripts/fetch.env
# Then it runs scripts/summarize_run.py on the copy, with the duration of run.yaml, and
# writes the CSV files used by the notebook into results/<run>/.
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
[ $# -eq 1 ] && [ "$1" != "-h" ] && [ "$1" != "--help" ] || { sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'; exit 0; }
RUN="$1"

ENV_FILE="$ROOT/scripts/fetch.env"
[ -f "$ENV_FILE" ] || { echo "Error: $ENV_FILE not found: cp scripts/fetch.env.example scripts/fetch.env" >&2; exit 1; }
# shellcheck disable=SC1090
. "$ENV_FILE"
: "${EDGE_SSH:?set EDGE_SSH in scripts/fetch.env}"
: "${CLOUD_SSH:?set CLOUD_SSH in scripts/fetch.env}"

OUT="$ROOT/results/$RUN"
mkdir -p "$OUT/logs"
echo "=== Copying run '$RUN' into $OUT ==="
scp -r "$EDGE_SSH:runs/$RUN/edge" "$OUT/logs/"
scp -r "$CLOUD_SSH:runs/$RUN/cloud" "$OUT/logs/"
scp "$CLOUD_SSH:runs/$RUN/*.csv" "$CLOUD_SSH:runs/$RUN/run.yaml" "$CLOUD_SSH:runs/$RUN/*.log" "$OUT/"

MINUTES=$(awk '/^minutes:/ {print $2}' "$OUT/run.yaml")
echo
echo "=== Summary ($MINUTES minutes) ==="
cd "$ROOT"
.venv/bin/python scripts/summarize_run.py "$OUT/logs/edge" "$OUT/logs/cloud" \
    --duration "$(( ${MINUTES:-60} * 60 ))" --out "$OUT"
