#!/bin/bash
# Produce the seed directory (the input of every stage) by running ONE job with
# SEED_CAPTURE=1 on a site where all stages run (e.g. placement all-edge).
#
#   scripts/capture-seeds.sh --gateway <edge-host>:30080 [--context <kubectl-context>]
#
# Each worker copies its input into /mnt/ramdisk/seed/<stage>/ on that node before
# processing. Run on that node, the script then copies the seeds into data/seed, to be
# committed: from then on every clone has them and scripts/setup-site.sh installs them.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NODE_SEEDS="/mnt/ramdisk/seed"

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

if [[ ! -d "$NODE_SEEDS" ]]; then
    echo "The seeds are in $NODE_SEEDS on the node that ran the job: copy them into data/seed."
    exit 0
fi
echo "=== Copying the seeds into data/seed ==="
# The first stage has no seed: its input is the video.
for s in "${STAGES[@]:1}"; do
    if [[ -z "$(ls -A "$NODE_SEEDS/$s" 2>/dev/null)" ]]; then
        echo "Error: no seed captured for $s in $NODE_SEEDS/$s (did the job complete?)" >&2; exit 1
    fi
done
rm -rf "$ROOT/data/seed"
mkdir -p "$ROOT/data/seed"
for s in "${STAGES[@]:1}"; do cp -r "$NODE_SEEDS/$s" "$ROOT/data/seed/$s"; done
du -sh "$ROOT/data/seed"/*
echo "Commit them:  git add data/seed && git commit -m 'Add the seeds'"
