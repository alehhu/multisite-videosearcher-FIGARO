#!/bin/bash
# Build the image of the scaling agents: the DQN agents of ServerlessAutoscalingWithRL,
# unchanged (code, RL4CC library and the six trained checkpoints).
#
#   scripts/build-agent-image.sh [--src <clone of ServerlessAutoscalingWithRL>]
#
# Without --src the repository is cloned into .cache/ at the commit below. The result is
# figaro-production_agent:latest, used by all six agents (deploy/k8s/agents/agents.yaml).
# x86-64 only: the requirements include the CUDA libraries of PyTorch (no GPU is needed).
set -euo pipefail

AGENT_REPO="https://github.com/sampozz/ServerlessAutoscalingWithRL.git"
AGENT_COMMIT="0414a043831c1c66d325e4b2ec69f3e6fe9026ab"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --src) SRC="${2:-}"; shift 2 ;;
        -h|--help) sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Error: unknown argument '$1'" >&2; exit 1 ;;
    esac
done

if [[ -z "$SRC" ]]; then
    SRC="$ROOT/.cache/ServerlessAutoscalingWithRL"
    if [[ ! -d "$SRC/.git" ]]; then
        echo "Cloning $AGENT_REPO"
        mkdir -p "$ROOT/.cache"
        git clone --quiet "$AGENT_REPO" "$SRC"
    fi
    git -C "$SRC" cat-file -e "$AGENT_COMMIT^{commit}" 2>/dev/null || git -C "$SRC" fetch --quiet origin
    git -C "$SRC" checkout --quiet --detach "$AGENT_COMMIT"
fi
AGENT_DIR="$SRC/figaro/agent"
DQN="src/production_agents/DQN"
if [[ ! -f "$AGENT_DIR/$DQN/Dockerfile" ]]; then
    echo "Error: $AGENT_DIR/$DQN/Dockerfile not found" >&2; exit 1
fi

cd "$AGENT_DIR"
echo "Building rl4cc-base:latest"
docker build -t rl4cc-base:latest -f "$DQN/Dockerfile.base" .
echo "Building figaro-production_agent:latest"
docker build -t figaro-production_agent:latest -f "$DQN/Dockerfile" .
echo "Agent image built."
