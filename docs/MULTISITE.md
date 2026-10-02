# Running the multi-site setup

```
client --HTTP :30080--> EDGE                        <==MQTT bridge :31883==>  CLOUD
                        gateway, collector                                    placement coordinator
                        broker, controller, stages                            broker, controller, stages
```

## Prerequisites (both servers)

- k3s (ships metrics-server, needed for CPU metrics and HPA).
- chrony running and synchronised: per-stage times mix the two clocks.
  Check with `chronyc tracking` (offset should be well under 1 ms).
- Reachability (provided by the SSH tunnel of step 0 if there is no direct route):
  - client → edge `30080/tcp` (gateway);
  - one broker → the other broker `31883/tcp` (MQTT bridge, either direction);
  - client → cloud `31883/tcp` (placement coordinator).
- Measure the link before anything else: `ping` and `iperf3` between the two servers.
- Decide the edge CPU budget (the edge should not be larger than the cloud).

## 0. Link between the sites (SSH tunnel)

Needed when the two servers cannot reach each other directly but the edge can reach the
cloud VM over SSH (possibly through jump hosts). On the edge server, with k3s already
installed (the tunnel binds to the k3s bridge address):

```bash
cp scripts/tunnel.env.example scripts/tunnel.env    # fill in the hosts, once
tmux new -s tunnel
scripts/tunnel.sh        # edge 10.42.0.1:41883 -> cloud broker, VM localhost:38080 -> edge gateway
```

ssh asks for the credentials of each hop. When it stops printing, the tunnel is up: detach
with `Ctrl-b d`. Check it from another shell on the edge server:

```bash
nc -zv 10.42.0.1 41883      # cloud broker reachable through the tunnel
```

Then use `--bridge-to 10.42.0.1:41883` when deploying the edge, and
`http://localhost:38080` as the gateway address for clients running on the cloud VM.
The tunnel does not reconnect by itself: re-run it after a network drop.

Measure the link before running experiments, at a realistic message rate:

```bash
ping -c 50 <first-jump-host>                                              # on the edge server
scripts/link_probe.py --broker localhost:31883 --from edge --to cloud --interval 0.5
scripts/link_probe.py --broker localhost:31883 --from edge --to edge  --interval 0.5   # local baseline
```

## 1. Build the images on each server

```bash
./scripts/build-all-images.sh
# k3s uses containerd: import the images
for i in ffmpeg0 librosa ffmpeg1 ffmpeg2 deepspeech grep gateway timing-collector local-controller; do
  docker save $i:latest | sudo k3s ctr images import -
done
```

## 2. Prepare the node and deploy each site

On each server (writes `/mnt/ramdisk/video.mp4`, and the seeds once they exist):

```bash
./scripts/deploy-site.sh --site cloud --prepare-node
./scripts/deploy-site.sh --site edge  --prepare-node \
                         --bridge-to <cloud-host>:31883          # the edge opens the bridge
```

Put `--bridge-to` on exactly one site: the one that can reach the other's broker. If only
the cloud can reach the edge, use `--bridge-to <edge-host>:31883` on the cloud deploy instead.

## 3. Capture the seeds (once)

The seeds are the input files of every stage, used when a job arrives from the other site.

```bash
scripts/placement.py publish placements/all-edge.yaml --broker <cloud-host>:31883
scripts/capture-seeds.sh --gateway <edge-host>:30080
# on the edge: copy the seeds to the cloud node, then install them there
rsync -a /mnt/ramdisk/seed/ <cloud-node>:/tmp/seed/
./scripts/deploy-site.sh --site cloud --prepare-node --seeds /tmp/seed   # on the cloud node
```

## 4. Choose a placement

```bash
scripts/placement.py publish placements/split-ffmpeg2.yaml --broker <cloud-host>:31883
scripts/placement.py show --broker <cloud-host>:31883
```

`publish` returns once the new table is in force; the old site keeps draining in the
background (see `control/status/<site>` via `show`). Any YAML with the same shape works.

## 5. Run the load and collect the logs

```bash
curl -X POST http://<edge-host>:30080/workflow/invoke/videosearcher_workflow \
     -H 'Content-Type: application/json' -d '{"params": {"dir": "test-1"}, "Async": false}'
```

Locust: `load/locust/locust_test.py` (set the host/port of the edge gateway).

Logs on each node, under `/var/log/multisite/`:

| File | Site | Content |
|---|---|---|
| `timing.jsonl` | edge | one record per finished job: per-stage start/end, site, pod, placement version, `wait`, errors |
| `pods.csv` | both | every 5 s, per stage: desired, ready, alive, terminating, hpa, placed_here, draining |
| `transitions.jsonl` | both | each step of each placement change |
| `observations.jsonl` | both | every 120 s: agent-ready state per stage (DQN) and per site (master) |

For resources and cost use `alive` (it includes pods that are shutting down but still working),
not `desired` or `ready`.

## Local test without servers

```bash
python -m venv .venv && .venv/bin/pip install paho-mqtt==2.1.0 pyyaml flask==3.0.0 kubernetes
colima start   # or any Docker daemon
.venv/bin/python tests/integration/run_integration.py --bridge-from edge
.venv/bin/python tests/integration/run_integration.py --bridge-from cloud
```

Two real brokers with the real bridge config, the real gateway, collector and controllers,
fake stage workers. See the docstring of `run_integration.py` for what it checks.
