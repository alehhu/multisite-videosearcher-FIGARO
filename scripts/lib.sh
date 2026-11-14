# Shared by the run scripts (rebuild-stages, prepare-run, monitor-run, run-experiment):
# settings of this server and small helpers. Sourced, not run.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NS=multisite
STAGES=(ffmpeg0 librosa ffmpeg1 ffmpeg2 deepspeech grep)
LOG_DIR=/var/log/multisite
RAMDISK=/mnt/ramdisk
RUNS_DIR="${RUNS_DIR:-$HOME/runs}"

die() { echo "Error: $*" >&2; exit 1; }
step() { echo; echo "=== $1 ==="; }

# SITE, BRIDGE_TO and AGENTS from site.env; kubectl pointed at k3s.
load_site() {
    [[ -f "$ROOT/site.env" ]] || die "site.env not found in $ROOT"
    # shellcheck disable=SC1091
    source "$ROOT/site.env"
    SITE="${SITE:-}"
    AGENTS="${AGENTS:-false}"
    [[ "$SITE" == "edge" || "$SITE" == "cloud" ]] || die "SITE in site.env must be edge or cloud"
    # The kubectl of k3s reads /etc/rancher/k3s/k3s.yaml (root only) unless KUBECONFIG is set.
    if [[ -z "${KUBECONFIG:-}" && -f "$HOME/.kube/config" ]]; then
        export KUBECONFIG="$HOME/.kube/config"
    fi
}

# Desired replicas of a deployment, 0 if it does not exist.
replicas() {
    local n
    n=$(kubectl -n "$NS" get deployment "$1" -o jsonpath='{.spec.replicas}' 2>/dev/null) || n=0
    echo "${n:-0}"
}

# The stages that run on this site now (deployment scaled above 0), one per line.
running_stages() {
    local s
    for s in "${STAGES[@]}"; do
        if [[ "$(replicas "$s-deployment")" -gt 0 ]]; then echo "$s"; fi
    done
}

# Deployment of an image: the stages are <stage>-deployment, the others have their own names.
deployment_of() {
    case "$1" in
        gateway) echo gateway-deployment ;;
        timing-collector|local-controller|dispatcher) echo "$1" ;;
        *) echo "$1-deployment" ;;
    esac
}
