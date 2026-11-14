#!/bin/bash
# On your machine: copy a run from the two servers into results/<run>/ and summarise it.
#
#   scripts/fetch-run.sh <run>
#
# The SSH hosts of the two servers (as you log in to them, jump hosts in ~/.ssh/config)
# come from scripts/fetch.env, not tracked:
#   cp scripts/fetch.env.example scripts/fetch.env
# One connection per server. Run it once both monitors have saved their logs (the end of
# the run plus 5 minutes): without them it stops and copies nothing. Then it runs
# scripts/summarize_run.py on the copy, with the duration of run.yaml, and writes the CSV
# files used by the notebook into results/<run>/.
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
[ $# -eq 1 ] && [ "$1" != "-h" ] && [ "$1" != "--help" ] || { sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'; exit 0; }
RUN="$1"

ENV_FILE="$ROOT/scripts/fetch.env"
[ -f "$ENV_FILE" ] || { echo "Error: $ENV_FILE not found: cp scripts/fetch.env.example scripts/fetch.env" >&2; exit 1; }
# shellcheck disable=SC1090
. "$ENV_FILE"
: "${EDGE_SSH:?set EDGE_SSH in scripts/fetch.env}"
: "${CLOUD_SSH:?set CLOUD_SSH in scripts/fetch.env}"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

echo "=== Copying run '$RUN' from the edge ($EDGE_SSH) ==="
ssh "$EDGE_SSH" "tar -C runs -cf - '$RUN/edge'" | tar -C "$TMP" -xf -
echo "=== Copying run '$RUN' from the cloud ($CLOUD_SSH) ==="
ssh "$CLOUD_SSH" "tar -C runs -cf - '$RUN'" | tar -C "$TMP" -xf -

# The monitors copy the logs only at the end: without them the run is not finished.
missing=""
[ -s "$TMP/$RUN/edge/timing.jsonl" ] || missing="$missing edge/timing.jsonl"
[ -f "$TMP/$RUN/cloud/pods.csv" ] || missing="$missing cloud/pods.csv"
[ -f "$TMP/$RUN/run.yaml" ] || missing="$missing run.yaml"
if [ -n "$missing" ]; then
    echo "Error: missing$missing." >&2
    echo "The monitors save the logs at the end of the run plus 5 minutes: wait for" >&2
    echo "\"logs of edge saved\" on the edge and \"Run '$RUN' saved\" on the cloud VM, then retry." >&2
    exit 1
fi

OUT="$ROOT/results/$RUN"
mkdir -p "$OUT/logs"
rm -rf "$OUT/logs/edge" "$OUT/logs/cloud"
mv "$TMP/$RUN/edge" "$TMP/$RUN/cloud" "$OUT/logs/"
cp -R "$TMP/$RUN/." "$OUT/"
echo "Copied into $OUT"

MINUTES=$(awk '/^minutes:/ {print $2}' "$OUT/run.yaml")
echo
echo "=== Summary ($MINUTES minutes) ==="
cd "$ROOT"
.venv/bin/python scripts/summarize_run.py "$OUT/logs/edge" "$OUT/logs/cloud" \
    --duration "$(( ${MINUTES:-60} * 60 ))" --out "$OUT"
