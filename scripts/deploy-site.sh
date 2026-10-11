#!/bin/bash
# Deploy one site (its own k3s cluster) of the multi-site pipeline.
#
#   scripts/deploy-site.sh --site edge  [--bridge-to <cloud-host>:31883] [--context <kubectl-context>]
#   scripts/deploy-site.sh --site cloud [--bridge-to <edge-host>:31883]  [--context <kubectl-context>]
#
# --bridge-to goes on exactly ONE of the two sites: the one that can reach the other
# site's broker (NodePort 31883). It opens the MQTT bridge that carries jobs, placement
# and status in both directions (deploy/mosquitto/bridge-from-<site>.conf).
#
# --prepare-node (run on the site's node itself) creates /mnt/ramdisk with the input video
# and the seeds from data/seed (or from --seeds <dir>), see scripts/capture-seeds.sh.
set -euo pipefail

usage() { sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'; }

SITE=""
BRIDGE_TO=""
CONTEXT_ARGS=()
PREPARE_NODE="false"
SEED_SRC=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --site) SITE="${2:-}"; shift 2 ;;
        --bridge-to) BRIDGE_TO="${2:-}"; shift 2 ;;
        --context) CONTEXT_ARGS=(--context "${2:-}"); shift 2 ;;
        --prepare-node) PREPARE_NODE="true"; shift ;;
        --seeds) SEED_SRC="${2:-}"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Error: unknown argument '$1'" >&2; usage; exit 1 ;;
    esac
done

if [[ "$SITE" != "edge" && "$SITE" != "cloud" ]]; then
    echo "Error: --site must be edge or cloud" >&2; usage; exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NS=multisite
KUBECTL=(kubectl "${CONTEXT_ARGS[@]}")

if [[ "$PREPARE_NODE" == "true" ]]; then
    # Run this on the site's node itself (it writes to the host filesystem).
    echo "=== Preparing /mnt/ramdisk on this node ==="
    sudo mkdir -p /mnt/ramdisk /var/log/multisite
    sudo chmod 777 /mnt/ramdisk /var/log/multisite
    cp "$ROOT/data/video.mp4" /mnt/ramdisk/video.mp4
    SEEDS="${SEED_SRC:-$ROOT/data/seed}"
    if [[ -d "$SEEDS" ]]; then
        echo "Seeds from $SEEDS"
        # sudo: seeds captured on this node were written by the pods, as root.
        sudo rm -rf /mnt/ramdisk/seed
        sudo cp -r "$SEEDS" /mnt/ramdisk/seed
        sudo chmod -R a+rX /mnt/ramdisk/seed
    elif [[ -n "$SEED_SRC" ]]; then
        echo "Error: $SEED_SRC is not a directory" >&2; exit 1
    else
        echo "No seeds in data/seed yet: jobs cannot move between sites until they are"
        echo "captured (scripts/capture-seeds.sh). /mnt/ramdisk/seed is left as it is."
    fi
    ls -la /mnt/ramdisk /mnt/ramdisk/seed 2>/dev/null || true
fi

echo "=== Applying overlay $SITE ==="
"${KUBECTL[@]}" apply -k "$ROOT/deploy/k8s/overlays/$SITE"

if [[ -n "$BRIDGE_TO" ]]; then
    [[ "$BRIDGE_TO" == *:* ]] || BRIDGE_TO="$BRIDGE_TO:31883"
    OTHER=$([[ "$SITE" == "edge" ]] && echo CLOUD || echo EDGE)
    echo "=== Configuring the bridge from $SITE to $BRIDGE_TO ==="
    TMP="$(mktemp -d)"
    sed "s|${OTHER}_BROKER_ADDRESS:1883|${BRIDGE_TO}|" \
        "$ROOT/deploy/mosquitto/bridge-from-$SITE.conf" > "$TMP/bridge.conf"
    "${KUBECTL[@]}" -n "$NS" create configmap mosquitto-bridge --from-file=bridge.conf="$TMP/bridge.conf" \
        --dry-run=client -o yaml | "${KUBECTL[@]}" -n "$NS" apply -f -
    rm -rf "$TMP"
    "${KUBECTL[@]}" -n "$NS" rollout restart deployment/mosquitto-deployment
fi

"${KUBECTL[@]}" -n "$NS" rollout status deployment/mosquitto-deployment --timeout=120s
"${KUBECTL[@]}" -n "$NS" rollout status deployment/local-controller --timeout=120s
echo "Site $SITE deployed. Stages start once a placement is published:"
echo "  scripts/placement.py publish placements/all-edge.yaml --broker <cloud-host>:31883"
