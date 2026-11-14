#!/bin/bash
# Rebuild images after a `git pull`, import them into k3s and restart what uses them,
# without touching anything else (replicas, agents, placement).
#
#   scripts/rebuild-stages.sh                    # the stages that run on this site now
#   scripts/rebuild-stages.sh --all              # all six stages
#   scripts/rebuild-stages.sh grep timing-collector
#
# Names: the six stages, gateway, timing-collector, local-controller, dispatcher. Only
# the deployments scaled above 0 here are restarted; the others get the new image when
# they next start. At the end it checks that every restarted stage runs the worker of
# this checkout. It uses sudo to import the images into k3s.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() { sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'; }

NAMES=()
ALL="false"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --all) ALL="true"; shift ;;
        -h|--help) usage; exit 0 ;;
        -*) echo "Error: unknown option '$1'" >&2; usage; exit 1 ;;
        *) NAMES+=("$1"); shift ;;
    esac
done

load_site
cd "$ROOT"

if [[ "$ALL" == "true" ]]; then
    NAMES=("${STAGES[@]}")
elif [[ ${#NAMES[@]} -eq 0 ]]; then
    while read -r s; do NAMES+=("$s"); done < <(running_stages)
    [[ ${#NAMES[@]} -gt 0 ]] || die "no stage runs on this site: name the images to rebuild"
fi

build() {
    case "$1" in
        gateway) docker build -t gateway:latest -f gateway/Dockerfile . ;;
        timing-collector) docker build -t timing-collector:latest -f monitoring/timing_collector/Dockerfile monitoring/timing_collector/ ;;
        local-controller) docker build -t local-controller:latest -f controller/Dockerfile . ;;
        dispatcher) docker build -t dispatcher:latest -f dispatcher/Dockerfile . ;;
        *)
            [[ -f "functions/$1/Dockerfile" ]] || die "unknown image '$1'"
            docker build -t "$1:latest" -f "functions/$1/Dockerfile" . ;;
    esac
}

step "Building ${NAMES[*]} (site $SITE, at $(git log --oneline -1))"
for name in "${NAMES[@]}"; do
    echo "--- $name"
    build "$name"
done

step "Importing into k3s"
for name in "${NAMES[@]}"; do
    echo "$name:latest"
    docker save "$name:latest" | sudo k3s ctr images import - >/dev/null
done

RESTARTED=()
for name in "${NAMES[@]}"; do
    deployment=$(deployment_of "$name")
    if [[ "$(replicas "$deployment")" -gt 0 ]]; then RESTARTED+=("$name"); fi
done

if [[ ${#RESTARTED[@]} -gt 0 ]]; then
    step "Restarting ${RESTARTED[*]}"
    for name in "${RESTARTED[@]}"; do
        kubectl -n "$NS" rollout restart "deployment/$(deployment_of "$name")"
    done
    # A stage that is shutting down first finishes the jobs it already has: this can take long.
    for name in "${RESTARTED[@]}"; do
        kubectl -n "$NS" rollout status "deployment/$(deployment_of "$name")" --timeout=1800s
    done
else
    step "Nothing to restart: none of these runs on this site"
fi

FAILED=0
EXPECTED=$(md5sum common/pipeline_worker.py | cut -d' ' -f1)
CHECKED=()
for name in "${RESTARTED[@]}"; do
    case "$name" in gateway|timing-collector|local-controller|dispatcher) ;; *) CHECKED+=("$name") ;; esac
done
[[ ${#CHECKED[@]} -eq 0 ]] || step "Checking the worker in the pods"
for name in "${CHECKED[@]}"; do
    got=$(kubectl -n "$NS" exec "deploy/$name-deployment" -- md5sum /opt/videosearcher/pipeline_worker.py | cut -d' ' -f1)
    if [[ "$got" == "$EXPECTED" ]]; then
        echo "  ok    $name"
    else
        echo "  WRONG $name runs another pipeline_worker.py ($got, expected $EXPECTED)"
        FAILED=1
    fi
done

step "Removing the old images from Docker"
docker image prune -f >/dev/null
df -h / | tail -1
[[ "$FAILED" -eq 0 ]] || die "some pods do not run the new worker"
echo "Done."
