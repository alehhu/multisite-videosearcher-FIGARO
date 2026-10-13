#!/bin/bash
# Set up one site of the multi-site pipeline on this server, from a fresh clone.
#
#   edit site.env                     # say which site this is, once
#   scripts/setup-site.sh
#
#   scripts/setup-site.sh --check        # only check the prerequisites
#   scripts/setup-site.sh --skip-build   # images already built and imported
#
# Steps: check the prerequisites, build the images, import them into k3s, create the
# Python environment for the scripts, prepare /mnt/ramdisk, deploy the site (the MQTT
# bridge if BRIDGE_TO is set, the scaling agents if AGENTS is true). It can be run
# again at any time, but not during an experiment: it restarts the pods.
#
# Nothing is installed on the server: a missing prerequisite stops the script with the
# command to run. sudo is used only to import the images into k3s and to create
# /mnt/ramdisk and /var/log/multisite.
set -euo pipefail

usage() { sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; }

CHECK_ONLY="false"
SKIP_BUILD="false"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --check) CHECK_ONLY="true"; shift ;;
        --skip-build) SKIP_BUILD="true"; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Error: unknown argument '$1'" >&2; usage; exit 1 ;;
    esac
done

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# The kubectl of k3s reads /etc/rancher/k3s/k3s.yaml (root only) unless KUBECONFIG is set.
if [[ -z "${KUBECONFIG:-}" && -f "$HOME/.kube/config" ]]; then
    export KUBECONFIG="$HOME/.kube/config"
fi
NS=multisite
IMAGES=(ffmpeg0 librosa ffmpeg1 ffmpeg2 deepspeech grep gateway timing-collector local-controller)

if [[ ! -f "$ROOT/site.env" ]]; then
    echo "Error: site.env not found. Restore it with: git checkout site.env" >&2; exit 1
fi
# shellcheck disable=SC1091
source "$ROOT/site.env"
SITE="${SITE:-}"
BRIDGE_TO="${BRIDGE_TO:-}"
AGENTS="${AGENTS:-false}"
if [[ "$SITE" != "edge" && "$SITE" != "cloud" ]]; then
    echo "Error: SITE in site.env must be edge or cloud" >&2; exit 1
fi
if [[ "$AGENTS" != "true" && "$AGENTS" != "false" ]]; then
    echo "Error: AGENTS in site.env must be true or false" >&2; exit 1
fi
STAGES=(ffmpeg0 librosa ffmpeg1 ffmpeg2 deepspeech grep)
[[ "$AGENTS" == "true" ]] && IMAGES+=(figaro-production_agent)

step() { echo; echo "=== $1 ==="; }

# Prerequisites: every problem is listed before stopping, each with the command to run.
ERRORS=()
WARNINGS=()
ok()   { echo "  ok       $1"; }
fail() { echo "  MISSING  $1"; ERRORS+=("$1: $2"); }
warn() { echo "  warning  $1"; WARNINGS+=("$1: $2"); }

step "Checking the prerequisites (site: $SITE, agents: $AGENTS)"

if [[ "$(uname -m)" == "x86_64" ]]; then ok "x86-64 machine"
else fail "x86-64 machine" "this is $(uname -m); the base image of the stages exists only for amd64"; fi

# Docker is only needed to build the images.
docker_problem() { if [[ "$SKIP_BUILD" == "true" ]]; then warn "$@"; else fail "$@"; fi; }
if ! command -v docker >/dev/null; then
    docker_problem "docker" "install Docker Engine (https://docs.docker.com/engine/install/, the page of this distribution)"
elif ! docker info >/dev/null 2>&1; then
    docker_problem "docker usable by $USER" "start the daemon, or: sudo usermod -aG docker $USER  (then log in again on a new SSH connection; to run now: sg docker -c '$0')"
else ok "docker"; fi

if ! command -v k3s >/dev/null; then
    fail "k3s" "curl -sfL https://get.k3s.io | sh -"
elif command -v systemctl >/dev/null && ! systemctl is-active --quiet k3s; then
    fail "k3s running" "it is installed but not running: sudo journalctl -u k3s -n 30  (another Kubernetes on this machine, e.g. MicroK8s, uses the same ports)"
elif ! command -v kubectl >/dev/null || ! kubectl get nodes >/dev/null 2>&1; then
    fail "kubectl access to k3s" "mkdir -p ~/.kube && sudo k3s kubectl config view --raw > ~/.kube/config && chmod 600 ~/.kube/config && echo 'export KUBECONFIG=\$HOME/.kube/config' >> ~/.bashrc  (it replaces ~/.kube/config: keep a copy if there is one)"
elif ! kubectl get nodes --no-headers | awk '{print $2}' | grep -qx Ready; then
    fail "k3s node Ready" "kubectl get nodes; sudo systemctl status k3s"
else
    ok "k3s ($(kubectl config current-context))"
    if kubectl -n kube-system get deployment metrics-server >/dev/null 2>&1; then ok "metrics-server"
    else warn "metrics-server" "not found: CPU utilisation will be logged as 0"; fi
fi

if ! python3 -c 'import venv, ensurepip' >/dev/null 2>&1; then
    fail "python3 with venv" "sudo apt install -y python3-venv"
else ok "python3 with venv"; fi

if [[ -f "$ROOT/data/video.mp4" ]]; then ok "data/video.mp4"
else fail "data/video.mp4" "missing from the clone: git checkout data/video.mp4"; fi

# Per-stage times mix the clocks of the two sites.
if ! command -v chronyc >/dev/null; then
    warn "chrony" "not installed (needed when two sites are used): sudo apt install -y chrony"
elif ! chronyc tracking 2>/dev/null | grep -q "Leap status *: Normal"; then
    warn "chrony" "clock not synchronised: chronyc tracking"
else ok "chrony synchronised"; fi

FREE_GB=$(df -Pk /var/lib | awk 'NR==2 {print int($4/1024/1024)}')
if (( FREE_GB < 20 )); then warn "disk space" "only ${FREE_GB} GB free under /var/lib (images are stored twice: Docker and k3s)"
else ok "disk space (${FREE_GB} GB free)"; fi

if (( ${#WARNINGS[@]} > 0 )); then
    echo; echo "Warnings:"; printf '  - %s\n' "${WARNINGS[@]}"
fi
if (( ${#ERRORS[@]} > 0 )); then
    echo; echo "Cannot continue:" >&2; printf '  - %s\n' "${ERRORS[@]}" >&2
    exit 1
fi
if [[ "$CHECK_ONLY" == "true" ]]; then
    echo; echo "Prerequisites satisfied."; exit 0
fi

# Pods keep running the image they started with: restart them if the site was already up.
ALREADY_DEPLOYED="false"
kubectl get namespace "$NS" >/dev/null 2>&1 && ALREADY_DEPLOYED="true"

if [[ "$SKIP_BUILD" == "true" ]]; then
    step "Skipping the image build"
else
    step "Building the images"
    "$ROOT/scripts/build-all-images.sh"
    if [[ "$AGENTS" == "true" ]]; then
        step "Building the image of the scaling agents"
        "$ROOT/scripts/build-agent-image.sh"
    fi

    step "Importing the images into k3s"
    for image in "${IMAGES[@]}"; do
        echo "$image:latest"
        docker save "$image:latest" | sudo k3s ctr images import - >/dev/null
    done
fi

step "Python environment for the scripts (.venv)"
[[ -x "$ROOT/.venv/bin/python" ]] || python3 -m venv "$ROOT/.venv"
"$ROOT/.venv/bin/pip" install --quiet paho-mqtt==2.1.0 PyYAML==6.0.2
echo ".venv ready"

step "Deploying site $SITE"
DEPLOY_ARGS=(--site "$SITE" --prepare-node)
[[ -n "$BRIDGE_TO" ]] && DEPLOY_ARGS+=(--bridge-to "$BRIDGE_TO")
[[ "$AGENTS" == "true" ]] && DEPLOY_ARGS+=(--agents)
"$ROOT/scripts/deploy-site.sh" "${DEPLOY_ARGS[@]}"

if [[ "$ALREADY_DEPLOYED" == "true" && "$SKIP_BUILD" != "true" ]]; then
    step "Restarting the pods on the new images"
    kubectl -n "$NS" rollout restart deployment
    kubectl -n "$NS" rollout status deployment/mosquitto-deployment --timeout=120s
    kubectl -n "$NS" rollout status deployment/local-controller --timeout=120s
fi
if [[ "$SITE" == "edge" ]]; then
    kubectl -n "$NS" rollout status deployment/gateway-deployment --timeout=120s
    kubectl -n "$NS" rollout status deployment/timing-collector --timeout=120s
fi

if [[ "$AGENTS" == "true" ]]; then
    step "Waiting for the scaling agents"
    for stage in "${STAGES[@]}"; do
        if ! kubectl -n "$NS" rollout status "deployment/agent-$stage" --timeout=600s; then
            kubectl -n "$NS" get pods -l tier=agent
            echo "Error: agent-$stage did not start. See: kubectl -n $NS logs deployment/agent-$stage" >&2
            exit 1
        fi
    done

    step "Asking each agent for one action"
    # From the controller's pod: the same path the controller uses during an experiment.
    kubectl -n "$NS" exec -i deployment/local-controller -- python3 - "${STAGES[@]}" <<'PYTHON' \
        || echo "warning: the agents did not all answer; the controller keeps the replicas unchanged when they do not"
import json, sys, urllib.request
observation = {"n_instances": 0.1, "utilization": 0.5, "pressure": 0.5,
               "queue_length_dominant": 0.0, "workload": 0.2}
failed = False
for stage in sys.argv[1:]:
    request = urllib.request.Request(f"http://agent-{stage}:5000/action",
                                     data=json.dumps({"observation": observation}).encode(),
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            print(f"  agent-{stage}: action {json.loads(response.read())['action']}")
    except Exception as exc:
        failed = True
        print(f"  agent-{stage}: FAILED {exc!r}")
sys.exit(1 if failed else 0)
PYTHON
fi

step "Site $SITE is up"
kubectl -n "$NS" get pods
echo
echo "The stages run only where a placement puts them. Publish one:"
echo "  two sites, once, to the cloud broker:"
echo "    .venv/bin/python scripts/placement.py publish placements/all-$SITE.yaml --broker <cloud-host>:31883"
echo "  this site alone:"
echo "    .venv/bin/python scripts/placement.py publish placements/all-$SITE.yaml --broker localhost:31883 --sites $SITE"
