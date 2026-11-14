#!/bin/bash
# Watch this server during a run, then save its logs.
#
#   scripts/monitor-run.sh <run> <minutes> [--grace <seconds>]
#
# Every minute it records the CPU and memory of every pod (top.csv) and, on the edge, the
# round trip of a few probes through the bridge (bridge.csv: 0 lost means the tunnel is
# up). After <minutes> plus the grace (default 300 s, for the jobs still in the pipeline)
# it copies the logs of /var/log/multisite next to them, in ~/runs/<run>/<site>/.
# It also saves the logs if stopped early with Ctrl-c.
#
# On the cloud VM scripts/run-experiment.sh starts it by itself; on the edge start it in
# tmux, just before the run, with the same minutes.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() { sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'; }

RUN=""
MINUTES=""
GRACE=300
while [[ $# -gt 0 ]]; do
    case "$1" in
        --grace) GRACE="${2:-}"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        -*) echo "Error: unknown option '$1'" >&2; usage; exit 1 ;;
        *) if [[ -z "$RUN" ]]; then RUN="$1"; else MINUTES="$1"; fi; shift ;;
    esac
done
[[ -n "$RUN" && "$MINUTES" =~ ^[0-9]+$ ]] || { usage; exit 1; }

load_site
cd "$ROOT"
OUT="$RUNS_DIR/$RUN/$SITE"
[[ ! -e "$OUT" ]] || die "$OUT already exists: choose another run name"
mkdir -p "$OUT"

save_logs() {
    cp "$LOG_DIR"/* "$OUT"/ 2>/dev/null || true
    echo "$(date '+%H:%M:%S') logs of $SITE saved in $OUT"
}
trap save_logs EXIT
trap 'exit 130' INT TERM   # so that the logs are saved when stopped early

echo "t,pod,cpu_millicores,memory_mib" > "$OUT/top.csv"
[[ "$SITE" == "edge" ]] && echo "t,probes,lost,rtt_median_ms" > "$OUT/bridge.csv"

END=$(( $(date +%s) + MINUTES * 60 + GRACE ))
echo "Monitoring $SITE for run '$RUN' until $(date -d "@$END" '+%H:%M:%S' 2>/dev/null || echo "$MINUTES min + ${GRACE}s")"
while (( $(date +%s) < END )); do
    t=$(date +%s)
    kubectl -n "$NS" top pods --no-headers 2>/dev/null \
        | awk -v t="$t" '{gsub("m", "", $2); gsub("Mi", "", $3); print t "," $1 "," $2 "," $3}' >> "$OUT/top.csv" || true
    if [[ "$SITE" == "edge" ]]; then
        probe=$(.venv/bin/python scripts/link_probe.py --broker localhost:31883 --from edge --to cloud -n 3 --interval 0.2 --timeout 5 2>&1 || true)
        lost=$(echo "$probe" | awk '/^lost:/ {print $2}')
        median=$(echo "$probe" | awk '/^round trip/ {for (i = 1; i <= NF; i++) if ($i ~ /^median=/) {sub("median=", "", $i); print $i}}')
        echo "$t,3,${lost:-0},${median:-}" >> "$OUT/bridge.csv"
        if [[ "${lost:-0}" -gt 0 ]]; then echo "$(date '+%H:%M:%S') WARNING: ${lost} of 3 probes lost: is the tunnel down?"; fi
    fi
    pause=$(( END - $(date +%s) )); (( pause > 60 )) && pause=60; (( pause < 1 )) && pause=1
    sleep "$pause" & wait $!   # in the background: a stop request is handled at once
done
