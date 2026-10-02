#!/bin/bash
# SSH tunnel between the two sites, opened from the edge server towards the cloud VM
# through one or more jump hosts. Use it when the two sites cannot reach each other
# directly but the edge can reach the cloud side over SSH.
#
#   cp scripts/tunnel.env.example scripts/tunnel.env    # fill in the hosts, once
#   tmux new -s tunnel
#   scripts/tunnel.sh
#
# ssh asks for the credentials of each hop. Once it is connected it prints nothing more:
# detach with Ctrl-b d. The tunnel does not reconnect by itself after a network drop.
#
# It carries:
#   EDGE_BIND:41883    -> cloud broker (NodePort 31883 on the cloud VM), for the MQTT bridge:
#                         scripts/deploy-site.sh --site edge --bridge-to <EDGE_BIND>:41883
#   VM 127.0.0.1:38080 -> edge gateway (NodePort 30080 on the edge server), for clients
#                         running on the cloud VM: http://localhost:38080
#
# EDGE_BIND must be reachable from the edge pods but not from outside: by default the k3s
# bridge address (cni0, 10.42.0.1), which exists only once k3s is installed. Port 31883
# cannot be reused locally: on the edge server it is the edge broker's NodePort.
set -euo pipefail

ENV_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tunnel.env"
# shellcheck disable=SC1090
[[ -f "$ENV_FILE" ]] && source "$ENV_FILE"

: "${JUMPS:?set JUMPS (comma-separated user@host jump hosts) in scripts/tunnel.env}"
: "${VM:?set VM (user@host of the cloud VM) in scripts/tunnel.env}"
: "${VM_ADDR:?set VM_ADDR (address of the cloud VM as seen from itself) in scripts/tunnel.env}"
EDGE_ADDR="${EDGE_ADDR:-$(hostname -I | awk '{print $1}')}"
EDGE_BIND="${EDGE_BIND:-10.42.0.1}"
BROKER_LOCAL_PORT="${BROKER_LOCAL_PORT:-41883}"
GATEWAY_REMOTE_PORT="${GATEWAY_REMOTE_PORT:-38080}"

echo "edge bind ${EDGE_BIND}:${BROKER_LOCAL_PORT} -> ${VM_ADDR}:31883 (cloud broker)"
echo "VM 127.0.0.1:${GATEWAY_REMOTE_PORT} -> ${EDGE_ADDR}:30080 (edge gateway)"

# ServerAlive*: detect a dead path within ~30 s and exit instead of hanging.
# ExitOnForwardFailure: fail loudly if a port is already taken.
exec ssh -N \
    -J "$JUMPS" \
    -o ServerAliveInterval=10 -o ServerAliveCountMax=3 \
    -o ExitOnForwardFailure=yes \
    -L "${EDGE_BIND}:${BROKER_LOCAL_PORT}:${VM_ADDR}:31883" \
    -R "127.0.0.1:${GATEWAY_REMOTE_PORT}:${EDGE_ADDR}:30080" \
    "$VM"
