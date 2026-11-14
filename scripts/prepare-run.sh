#!/bin/bash
# Get this server ready for a run: check it, then clean up after the previous one.
# Run it on EACH server before every run.
#
#   scripts/prepare-run.sh            # checks, then cleanup
#   scripts/prepare-run.sh --check    # checks only, nothing changes
#
# Checks: clock synchronised, /mnt/ramdisk a tmpfs with the video and the seeds (also as
# the pods see it), pods ready, disk below k3s's eviction threshold, and the link to the
# other site (the bridge from the edge, the gateway through the tunnel from the cloud).
# Cleanup: removes the pods that failed or were evicted, moves the logs of the previous
# run aside (to ~/runs/unsaved-<time>/, if not empty) and empties them, removes the job
# directories, restarts the agents. It ends by printing the placement in force.
# It uses sudo to empty the logs and to remove the job directories.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() { sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; }

CHECK_ONLY="false"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --check) CHECK_ONLY="true"; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Error: unknown argument '$1'" >&2; usage; exit 1 ;;
    esac
done

load_site
cd "$ROOT"

ERRORS=()
ok()   { echo "  ok       $1"; }
fail() { echo "  PROBLEM  $1"; ERRORS+=("$1: $2"); }
warn() { echo "  warning  $1: $2"; }

step "Checking the $SITE site"

# Clock: per-stage times and waits mix the clocks of the two servers.
offset=$(chronyc tracking 2>/dev/null | awk '/^System time/ {print $4}')
if [[ -z "$offset" ]]; then
    fail "clock" "chrony not running: sudo systemctl start chrony"
elif awk -v o="$offset" 'BEGIN {exit !(o < 0.001)}'; then
    ok "clock (offset ${offset} s)"
else
    fail "clock" "offset ${offset} s, more than 1 ms: chronyc tracking"
fi

# Ramdisk: a tmpfs, as the original setup; a reboot leaves a plain directory instead.
MOUNT_HINT="sudo mount -t tmpfs -o size=4G tmpfs $RAMDISK && sudo cp data/video.mp4 $RAMDISK/ && sudo cp -r data/seed $RAMDISK/seed, then restart the stages"
if [[ "$(findmnt -n -o FSTYPE "$RAMDISK" 2>/dev/null)" != "tmpfs" ]]; then
    fail "ramdisk" "$RAMDISK is not a tmpfs (after a reboot?): $MOUNT_HINT"
else
    missing=()
    [[ -f "$RAMDISK/video.mp4" ]] || missing+=("video.mp4")
    for s in "${STAGES[@]:1}"; do [[ -d "$RAMDISK/seed/$s" ]] || missing+=("seed/$s"); done
    if [[ ${#missing[@]} -eq 0 ]]; then ok "ramdisk (tmpfs, video and seeds)"
    else fail "ramdisk" "missing ${missing[*]}: sudo cp data/video.mp4 $RAMDISK/ && sudo cp -r data/seed $RAMDISK/"; fi
fi
first_stage=$(running_stages | sed -n 1p)   # not head: under pipefail it ends the script
if [[ -n "$first_stage" ]]; then
    seen=$(kubectl -n "$NS" exec "deploy/$first_stage-deployment" -- stat -f -c %T "$RAMDISK" 2>/dev/null || true)
    if [[ "$seen" == "tmpfs" ]]; then ok "the pods see the tmpfs ($first_stage)"
    else fail "ramdisk in the pods" "$first_stage sees '$seen': the stages started before the tmpfs was mounted, restart them"; fi
fi

# Pods: everything that should run is running and ready. Failed ones are removed below.
not_ready=$(kubectl -n "$NS" get pods --no-headers 2>/dev/null \
    | awk '$3 != "Evicted" && $3 != "Error" && $3 != "Completed" && $3 != "ContainerStatusUnknown" {split($2, r, "/"); if ($3 != "Running" || r[1] != r[2]) print $1 " (" $3 ", " $2 ")"}')
if [[ -z "$not_ready" ]]; then ok "pods ready"
else fail "pods" "not ready: $(echo "$not_ready" | tr '\n' ' ')"; fi
failed_pods=$(kubectl -n "$NS" get pods --no-headers --field-selector=status.phase=Failed 2>/dev/null | wc -l | tr -d ' ')

# Disk: above 85% k3s removes images and evicts pods.
used=$(df --output=pcent / | tail -1 | tr -dc '0-9')
if (( used >= 85 )); then fail "disk" "${used}% used: k3s evicts pods above 85%"
elif (( used >= 80 )); then warn "disk" "${used}% used, close to the 85% where k3s evicts pods"
else ok "disk (${used}% used)"; fi

# The other site.
if [[ "$SITE" == "edge" ]]; then
    if probe=$(.venv/bin/python scripts/link_probe.py --broker localhost:31883 --from edge --to cloud -n 5 --interval 0.2 --timeout 5 2>&1); then
        median=$(echo "$probe" | awk '/^round trip/ {for (i = 1; i <= NF; i++) if ($i ~ /^median=/) {sub("median=", "", $i); print $i}}')
        ok "bridge to the cloud (round trip ${median} ms)"
    else
        fail "bridge" "probes to the cloud lost: is the tunnel up? (ss -ltn | grep 41883; scripts/tunnel.sh in tmux)"
    fi
else
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://localhost:38080/ || true)
    if [[ "$code" != "000" ]]; then ok "edge gateway through the tunnel (localhost:38080)"
    else fail "edge gateway" "localhost:38080 does not answer: is the tunnel up on the edge?"; fi
fi

if [[ "$CHECK_ONLY" == "true" || ${#ERRORS[@]} -gt 0 ]]; then
    if [[ ${#ERRORS[@]} -gt 0 ]]; then
        echo; echo "Not ready:" >&2; printf '  - %s\n' "${ERRORS[@]}" >&2
        [[ "$CHECK_ONLY" == "true" ]] || echo "Nothing was cleaned up." >&2
        exit 1
    fi
    echo; echo "All checks passed."; exit 0
fi

step "Cleaning up"
if [[ "$failed_pods" -gt 0 ]]; then
    kubectl -n "$NS" delete pod --field-selector=status.phase=Failed >/dev/null
    echo "removed $failed_pods failed or evicted pods"
fi

if [[ -n "$(find "$LOG_DIR" -maxdepth 1 -type f -size +0 2>/dev/null)" ]]; then
    aside="$RUNS_DIR/unsaved-$(date +%Y%m%d-%H%M%S)/$SITE"
    mkdir -p "$aside" && cp "$LOG_DIR"/* "$aside"/
    echo "logs of the previous run copied to $aside"
fi
# Emptied, not removed: the controller keeps pods.csv open.
sudo truncate -s 0 "$LOG_DIR"/*
echo "logs emptied"

scripts/clean-ramdisk.sh --delete

if [[ "$AGENTS" == "true" ]]; then
    step "Restarting the agents"
    kubectl -n "$NS" rollout restart deployment -l tier=agent >/dev/null
    for d in $(kubectl -n "$NS" get deployment -l tier=agent -o name); do
        # Agents scaled to 0 by hand stay at 0: nothing to wait for.
        if [[ "$(replicas "${d#deployment.apps/}")" -gt 0 ]]; then
            kubectl -n "$NS" rollout status "$d" --timeout=600s
        fi
    done
fi

step "Placement in force"
.venv/bin/python scripts/placement.py show --broker localhost:31883 | sed -n 1p
echo; echo "Site $SITE ready for the run."
