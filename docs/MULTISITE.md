# Running the multi-site setup

```
client --HTTP :30080--> EDGE                        <==MQTT bridge :31883==>  CLOUD
                        gateway, collector                                    placement coordinator
                        broker, controller, stages                            broker, controller, stages
                        scaling agents                                        scaling agents
```

Each server is set up with one command, `scripts/setup-site.sh`, from a clone of this
repository. The order is: cloud, link between the sites, edge, then a placement.

## 1. Prerequisites (both servers)

`setup-site.sh` checks them and stops with the command to run if one is missing; it does
not install anything.

- An x86-64 machine: the base image of the stages and the agent image exist only for amd64.
- Docker, usable without sudo.
- k3s (ships metrics-server, needed for the CPU metrics), with `kubectl` working for your
  user: copy the k3s configuration to `~/.kube/config` and set `KUBECONFIG` to it, since the
  `kubectl` of k3s otherwise reads only the root-owned `/etc/rancher/k3s/k3s.yaml`.
- Python 3 with `venv`.
- chrony running and synchronised when two sites are used: per-stage times mix the two
  clocks. Check with `chronyc tracking` (offset should be well under 1 ms).
- Reachability (provided by the SSH tunnel of step 3 if there is no direct route):
  - client → edge `30080/tcp` (gateway);
  - one broker → the other broker `31883/tcp` (MQTT bridge, either direction);
  - placement coordinator → cloud `31883/tcp`.
- Disk space: every image is stored twice (Docker and k3s), and the agent image is large.
- Decide the edge CPU budget (the edge should not be larger than the cloud).

## 2. Set up a site

```bash
git clone <this repository> && cd multisite-videosearcher-FIGARO
$EDITOR site.env                  # set SITE, and BRIDGE_TO on one of the two sites
scripts/setup-site.sh
```

`site.env` comes with the values for a single site with fixed replicas, and explains
each setting. Edit it on each server and keep the edited copy local (do not commit it):

| Setting | Meaning |
|---|---|
| `SITE` | `edge` or `cloud` |
| `BRIDGE_TO` | `host:port` of the other site's broker, on exactly one site: the one that can reach the other. Empty on the other site, and for a single site |
| `AGENTS` | `false` (default): fixed replicas. `true`: the stages are scaled by the DQN agents |

The script builds the images (the six stages, gateway, timing collector, controller and,
with `AGENTS=true`, the agents), imports them into k3s, creates `.venv` for the scripts,
prepares `/mnt/ramdisk` (input video, seeds) and `/var/log/multisite`, deploys the site
and waits for it to be ready. With agents, it ends by asking each of them for one action.
It uses sudo only to import the images into k3s and to create those two directories.

```bash
scripts/setup-site.sh --check        # only check the prerequisites
scripts/setup-site.sh --skip-build   # images already built and imported
```

It can be run again at any time (after a `git pull`, or after changing `site.env`), but
not during an experiment: it restarts the pods.

A single server is enough to try the pipeline: set it up as `edge` (the site with the
gateway and the collector), leave `BRIDGE_TO` empty and publish `placements/all-edge.yaml`
with `--sites edge`, so that the coordinator does not wait for the cloud:

```bash
.venv/bin/python scripts/placement.py publish placements/all-edge.yaml --broker localhost:31883 --sites edge
```

## 3. Link between the sites (SSH tunnel)

Needed when the two servers cannot reach each other directly but the edge can reach the
cloud VM over SSH (possibly through jump hosts). Set up the cloud first. Then, on the edge
server, with k3s already installed (the tunnel binds to the k3s bridge address):

```bash
cp scripts/tunnel.env.example scripts/tunnel.env    # fill in the hosts, once
tmux new -s tunnel
scripts/tunnel.sh        # edge 10.42.0.1:41883 -> cloud broker, VM localhost:38080 -> edge gateway
```

ssh asks for the credentials of each hop. When it stops printing, the tunnel is up: detach
with `Ctrl-b d`. Check it from another shell on the edge server:

```bash
timeout 3 bash -c '</dev/tcp/10.42.0.1/41883' && echo open || echo closed     # the tunnel is listening
```

Then set `BRIDGE_TO="10.42.0.1:41883"` in the edge's `site.env` and run `setup-site.sh`
there. Clients running on the cloud VM reach the gateway at `http://localhost:38080`.
The tunnel does not reconnect by itself: re-run it after a network drop.

With a direct route there is no tunnel: put the other broker's `host:31883` in `BRIDGE_TO`
on the site that can reach it.

Measure the link before running experiments, at a realistic message rate:

```bash
ping -c 50 <first-jump-host>                                              # on the edge server
.venv/bin/python scripts/link_probe.py --broker localhost:31883 --from edge --to cloud --interval 0.5
.venv/bin/python scripts/link_probe.py --broker localhost:31883 --from edge --to edge  --interval 0.5   # local baseline
```

## 4. Seeds (once)

The seeds are the input files of every stage, used when a job arrives from the other
site: files are not transferred between the sites. They live in `data/seed/<stage>/` and
`setup-site.sh` installs them in `/mnt/ramdisk/seed/` on each server.

If `data/seed/` is already in the repository there is nothing to do. Otherwise capture
them once, on a server set up as `edge`, with all the stages placed there:

```bash
.venv/bin/python scripts/placement.py publish placements/all-edge.yaml --broker localhost:31883   # add --sites edge if the cloud is not up yet
scripts/capture-seeds.sh --gateway localhost:30080     # one job; copies the seeds into data/seed
git add data/seed && git commit -m "Add the seeds" && git push
```

On the other server: `git pull`, then `scripts/setup-site.sh --skip-build`.

## 5. Choose a placement

The placement says which site runs each stage. It is published once, to the cloud broker,
and reaches the edge through the bridge:

```bash
.venv/bin/python scripts/placement.py publish placements/split-ffmpeg2.yaml --broker <cloud-host>:31883
.venv/bin/python scripts/placement.py show --broker <cloud-host>:31883
```

`publish` returns once the new table is in force; the old site keeps draining in the
background (see `control/status/<site>` via `show`). Any YAML with the same shape works.
`publish` waits for every site to be ready: with a single site deployed, use that site's
own broker (`localhost:31883`) and add `--sites <site>`, or it waits for the other forever.

## 6. Scaling agents

With `AGENTS=true` each site runs six agents, one per stage (`agent-<stage>`, port 5000):
the DQN agents of ServerlessAutoscalingWithRL, built unchanged from that repository by
`scripts/build-agent-image.sh` (code, library and trained checkpoints). They do not need a
GPU.

Every `observation_interval_s` the controller of the site that hosts a stage sends

```
POST http://agent-<stage>:5000/action
     {"observation": {"n_instances", "utilization", "pressure", "queue_length_dominant", "workload"}}
```

(all normalised to 0..1). The state is computed as the controller of ServerlessAutoscalingWithRL
computes it on Serverledge: every 5 s a sample with the response and service time of the
last job the stage finished on this site (kept until the next one), the arrivals of the
last 60 s (six 10 s buckets) and the ready replicas; pressure, queue length and utilization
per sample; then the mean of the samples of the window. A stage with no samples in the
window is not asked. `observations.jsonl` also keeps, under `per_job`, the same times
averaged over the jobs of the window, for the analysis.

The controller scales the stage to the `action` in the reply, kept between
the `min` and `max` of the stage's `agent` block in `deploy/k8s/base/controller-config.yaml`.
If the agent does not answer, the replicas stay as they are. A stage that is being moved
to the other site is not rescaled until the move is complete; it starts on the new site
with the number of replicas it had on the old one, and the placement change waits for all
of them to be ready. The agents only decide: they are not trained during the experiment.

With `AGENTS=false` the agents are not deployed and every stage keeps the `replicas` of
`controller-config.yaml`. A stage can also be given a Kubernetes HPA instead of an agent
(see the comments in that file).

Each replica of a stage requests one CPU: set `max` to what the server can actually run.
The `res_time_threshold` values come from another experiment and must be recalibrated on
the times measured here.

## 7. Measure the service times

Before an experiment, and whenever a server changes, measure each stage without queueing:
jobs are sent one at a time, so every stage runs alone. Publish `all-edge` or `all-cloud`
first, to measure the site you want, then on the edge server:

```bash
.venv/bin/python scripts/profile_stages.py --gateway localhost:30080 -n 20 --csv profile-edge.csv
```

It prints mean, standard deviation, minimum and maximum of every stage, of the whole job
and of the overhead (the job's duration minus its stages: broker hops, bridge, and up to
0.1 s of polling in the gateway). The first job is left out (`--warmup`): the first run on
a fresh pod is slower. These are the times to compare with `res_time_threshold`.

## 8. Run an experiment

An experiment is one hour of load from Locust, on the cloud VM, against the edge gateway.
Everything about a run ends up in one directory on your machine, `results/<run>/` (ignored
by git: keep a backup of it):

```
results/<run>/
  logs/edge/, logs/cloud/   copies of /var/log/multisite of the two servers
  request_timestamps.csv    Locust: every request, send time and end-to-end response time
  workloadProfile.csv       Locust: the load it planned, requests per second
  jobs.csv, stages.csv,     written by summarize_run.py --out (see below)
  replicas.csv, agents.csv
  plots/                    written by the notebook
```

### Before the run

1. **Code up to date.** After a `git pull` that changed the stages, the controller or the
   agents, run `scripts/setup-site.sh` on each server (it rebuilds the images); otherwise
   nothing to do.
2. **Checks**, on each server:
   ```bash
   chronyc tracking | grep 'System time'     # well under 1 ms
   kubectl get pods -n multisite              # all Running
   ```
   and, on the edge, that the tunnel is up (step 3).
3. **Clean up the previous run**, on each server, once its logs are saved (see "After the
   run"):
   ```bash
   scripts/clean-ramdisk.sh --delete          # job directories; seed/ and video.mp4 stay
   sudo truncate -s 0 /var/log/multisite/*    # empty the logs
   ```
   Empty the log files with `truncate`, do not delete or move them: the controller keeps
   `pods.csv` open. `summarize_run.py` takes the first job in the logs as the start of the
   run, so logs left over from an earlier run would mix the two.
4. **Placement**, from the cloud VM:
   ```bash
   .venv/bin/python scripts/placement.py publish placements/split-ffmpeg2.yaml --broker localhost:31883
   .venv/bin/python scripts/placement.py show --broker localhost:31883
   ```

### The run

On the cloud VM, in a `tmux` session (the run lasts one hour):

```bash
cd load/locust
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt     # once
GATEWAY_HOST=localhost GATEWAY_PORT=38080 .venv/bin/locust -f locust_test.py --headless -u 1 -r 1 --run-time 60m
```

Port 38080 is the edge gateway through the tunnel (step 3). Locust sends about 380 jobs:
two 20-minute waves with no load in between, up to about 0.28 jobs/s. It stops after
60 minutes even if jobs are still in the pipeline; they are not counted. It writes
`request_timestamps.csv` and `workloadProfile.csv` in `load/locust/`.

A single job by hand, to check the pipeline:

```bash
curl -X POST http://localhost:38080/workflow/invoke/videosearcher_workflow \
     -H 'Content-Type: application/json' -d '{"params": {}, "Async": false}'
```

### After the run

1. **Save the logs on each server**, with the site's name (`edge` or `cloud`):
   ```bash
   mkdir -p ~/runs/<run>/<site> && cp /var/log/multisite/* ~/runs/<run>/<site>/
   ```
   and, on the cloud VM, Locust's files:
   ```bash
   cp load/locust/request_timestamps.csv load/locust/workloadProfile.csv ~/runs/<run>/
   ```
2. **Copy them to your machine**, into the repository (the SSH hosts are the ones you log in
   with, jump hosts included):
   ```bash
   mkdir -p results/<run>/logs
   scp -r <edge-ssh-host>:runs/<run>/edge results/<run>/logs/
   scp -r <cloud-ssh-host>:runs/<run>/cloud results/<run>/logs/
   scp '<cloud-ssh-host>:runs/<run>/*.csv' results/<run>/
   ```
3. **Summarise and check the run:**
   ```bash
   .venv/bin/python scripts/summarize_run.py results/<run>/logs/edge results/<run>/logs/cloud --out results/<run>
   ```
   The run lasts `--duration` seconds (default 3600) from its first job: only what ends
   inside it counts, and the jobs still in the pipeline at the end are reported apart. It
   prints the jobs sent, completed, failed and duplicated; per 5 minutes the job duration
   and the wait of every stage; the waits of the hops between the sites; the service time of
   every stage on every site; the decisions of the agents and the replicas that stayed not
   ready. It ends with four checks (no failed job, no duplicate, every agent answered,
   replicas ready in time) and exits with an error if one fails: then the run is to be
   looked at before being used.

   With `--out` it writes the CSV files used by the notebook, times in seconds from the start
   of the run: `jobs.csv` (one row per job), `stages.csv` (one row per job and stage: site,
   pod, service time, wait), `replicas.csv` (replicas of every stage on every site, every
   5 s) and `agents.csv` (every observation window: the state, what the agent received, its
   answer, the replicas applied).
4. **Plots and tables:** `analysis/run_analysis.ipynb`. The first time:
   ```bash
   python3 -m venv analysis/.venv && analysis/.venv/bin/pip install -r analysis/requirements.txt
   ```
   then
   ```bash
   analysis/.venv/bin/jupyter notebook analysis/run_analysis.ipynb
   ```
   set `RUNS` in the first cell (e.g. `{"old agents": "../results/<run>"}`; up to three runs
   to compare them) and run all the cells. Each section says what its plot shows and how
   to read it; the figures are also saved as PNG under `results/<run>/plots/`. Section 5 is
   the profiling: the service time of every stage and the thresholds it implies.
5. **Before the next run**, clean up as in "Before the run", step 3.

### The logs

Under `/var/log/multisite/` on each server:

| File | Site | Content |
|---|---|---|
| `timing.jsonl` | edge | one record per finished job: per-stage start/end, site, pod, placement version, `wait`, errors |
| `pods.csv` | both | every 5 s, per stage: desired, ready, alive, terminating, hpa, placed_here, draining |
| `transitions.jsonl` | both | each step of each placement change |
| `observations.jsonl` | both | every 120 s: state per stage and per site, with each agent's answer and the replicas applied |

The timings of all the stages travel with the job, so `timing.jsonl` is written only on
the gateway's site. For resources and cost use `alive` (it includes pods that are shutting
down but still working), not `desired` or `ready`.

## Local test without servers

```bash
python -m venv .venv && .venv/bin/pip install paho-mqtt==2.1.0 pyyaml flask==3.0.0 kubernetes pytest
colima start   # or any Docker daemon
.venv/bin/python -m pytest tests/unit monitoring/timing_collector
.venv/bin/python tests/integration/run_integration.py --bridge-from edge
.venv/bin/python tests/integration/run_integration.py --bridge-from cloud
```

Two real brokers with the real bridge config, the real gateway, collector and controllers,
fake stage workers and fake agents. See the docstring of `run_integration.py` for what it
checks.
