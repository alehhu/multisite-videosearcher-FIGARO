#!/bin/bash
# Run an experiment from the cloud VM: Locust against the edge gateway, through the tunnel.
#
#   scripts/run-experiment.sh <run> [minutes]       # default 60
#
# Before: scripts/prepare-run.sh on both servers, then on the edge, in tmux,
#   scripts/monitor-run.sh <run> <minutes>
# Run this in tmux too. It writes ~/runs/<run>/run.yaml (what was run: commit, placement,
# agents, load), starts scripts/monitor-run.sh for this site, runs Locust for <minutes>,
# then keeps watching for the grace period of the monitor (5 minutes) so that the jobs
# still in the pipeline end up in the logs, and saves Locust's files with them.
# Then, on your machine: scripts/fetch-run.sh <run>
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() { sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'; }

[[ $# -ge 1 && "${1:-}" != "-h" && "${1:-}" != "--help" ]] || { usage; exit 0; }
RUN="$1"
MINUTES="${2:-60}"
[[ "$MINUTES" =~ ^[0-9]+$ ]] || die "minutes must be a whole number"

load_site
[[ "$SITE" == "cloud" ]] || die "run this on the cloud VM (it sends the load to the edge through the tunnel)"
cd "$ROOT"
OUT="$RUNS_DIR/$RUN"
[[ ! -e "$OUT" ]] || die "$OUT already exists: choose another run name"

GATEWAY="localhost:38080"
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://$GATEWAY/" || true)
[[ "$code" != "000" ]] || die "the edge gateway does not answer on $GATEWAY: is the tunnel up?"
placement=$(.venv/bin/python scripts/placement.py show --broker localhost:31883 | sed -n 1p)
[[ "$placement" != "current: None" ]] || die "no placement in force: publish one first"

LOCUST_DIR="$ROOT/load/locust"
if [[ ! -x "$LOCUST_DIR/.venv/bin/locust" ]]; then
    step "Installing Locust (once)"
    python3 -m venv "$LOCUST_DIR/.venv"
    "$LOCUST_DIR/.venv/bin/pip" install --quiet -r "$LOCUST_DIR/requirements.txt"
fi

mkdir -p "$OUT"
commit=$(git rev-parse --short HEAD)
[[ -z "$(git status --porcelain --untracked-files=no)" ]] || commit="$commit (with local changes)"
cat > "$OUT/run.yaml" <<EOF
name: $RUN
started: $(date -Iseconds)
minutes: $MINUTES
commit: $commit
agents: $AGENTS
placement: "${placement#current: }"
load: locust_test.py, 1 user, gateway $GATEWAY
EOF
echo "Run '$RUN': $MINUTES minutes, commit $commit, agents $AGENTS"
echo "${placement}"

step "Starting the monitor"
"$ROOT/scripts/monitor-run.sh" "$RUN" "$MINUTES" > "$OUT/monitor-$SITE.log" 2>&1 &
MONITOR=$!
trap 'kill $MONITOR 2>/dev/null || true' INT TERM

step "Locust for $MINUTES minutes (started $(date '+%H:%M:%S'))"
rm -f "$LOCUST_DIR/request_timestamps.csv" "$LOCUST_DIR/workloadProfile.csv"
(cd "$LOCUST_DIR" && GATEWAY_HOST=localhost GATEWAY_PORT=38080 \
    .venv/bin/locust -f locust_test.py --headless -u 1 -r 1 --run-time "${MINUTES}m") \
    > "$OUT/locust.log" 2>&1 || echo "Locust exited with an error: see $OUT/locust.log"
cp "$LOCUST_DIR/request_timestamps.csv" "$LOCUST_DIR/workloadProfile.csv" "$OUT"/ 2>/dev/null \
    || echo "warning: Locust's CSV files not found in $LOCUST_DIR"
sent=$(($(wc -l < "$OUT/request_timestamps.csv" 2>/dev/null || echo 1) - 1))
echo "Locust done at $(date '+%H:%M:%S'): $sent requests"

step "Waiting for the jobs still in the pipeline (up to 5 minutes)"
wait "$MONITOR" || true
echo
echo "Run '$RUN' saved in $OUT."
echo "When the edge monitor has saved its logs too, on your machine: scripts/fetch-run.sh $RUN"
