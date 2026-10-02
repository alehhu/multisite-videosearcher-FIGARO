#!/bin/bash
# Produce the seed directory (the input of every stage) by running ONE job with
# SEED_CAPTURE=1 on a site where all stages run (e.g. placement all-edge).
#
#   scripts/capture-seeds.sh --gateway <edge-host>:30080 [--context <kubectl-context>]
#
# Each worker copies its input into /mnt/ramdisk/seed/<stage>/ on that node before
# processing. Afterwards copy the node's /mnt/ramdisk/seed to the other site, e.g.
#   rsync -a /mnt/ramdisk/seed/ <other-node>:/mnt/ramdisk/seed/
set -euo pipefail

GATEWAY=""
CONTEXT_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --gateway) GATEWAY="${2:-}"; shift 2 ;;
        --context) CONTEXT_ARGS=(--context "${2:-}"); shift 2 ;;
        *) echo "Error: unknown argument '$1'" >&2; exit 1 ;;
    esac
done
[[ -n "$GATEWAY" ]] || { echo "Error: --gateway host:port is required" >&2; exit 1; }

KUBECTL=(kubectl "${CONTEXT_ARGS[@]}" -n multisite)
STAGES=(ffmpeg0 librosa ffmpeg1 ffmpeg2 deepspeech grep)

echo "=== Enabling SEED_CAPTURE ==="
for s in "${STAGES[@]}"; do "${KUBECTL[@]}" set env "deployment/$s-deployment" SEED_CAPTURE=1; done
for s in "${STAGES[@]}"; do "${KUBECTL[@]}" rollout status "deployment/$s-deployment" --timeout=300s; done

echo "=== Running one job (synchronous) ==="
curl -sf -X POST "http://$GATEWAY/workflow/invoke/videosearcher_workflow" \
    -H "Content-Type: application/json" \
    -d '{"params": {"dir": "seed-capture"}, "Async": false}' | head -c 600; echo

echo "=== Disabling SEED_CAPTURE ==="
for s in "${STAGES[@]}"; do "${KUBECTL[@]}" set env "deployment/$s-deployment" SEED_CAPTURE-; done
echo "Seeds are in /mnt/ramdisk/seed on the node that ran the job."
