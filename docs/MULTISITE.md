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

An experiment is a period of load from Locust, on the cloud VM, against the edge gateway
(one hour; 15 minutes for a pilot). Scripts chain the steps; each one says in its header
what it does, and everything can also be done by hand with the commands of the previous
sections. Everything about a run ends up in one directory on your machine,
`results/<run>/` (ignored by git: keep a backup of it):

```
results/<run>/
  run.yaml                  what was run: start, minutes, commit, agents, placement, load
  logs/edge/, logs/cloud/   /var/log/multisite of the two servers, plus every minute the CPU
                            and memory of every pod (top.csv) and, from the edge, the round
                            trip through the bridge (bridge.csv)
  request_timestamps.csv    Locust: every request, send time and end-to-end response time
  workloadProfile.csv       Locust: the load it planned, requests per second
  locust.log, monitor-cloud.log
  jobs.csv, stages.csv,     written by summarize_run.py (see below)
  replicas.csv, agents.csv
  plots/                    written by the notebook
```

### Once

On your machine, the SSH hosts of the two servers (as you log in to them):

```bash
cp scripts/fetch.env.example scripts/fetch.env      # not tracked: fill in EDGE_SSH and CLOUD_SSH
```

### After a `git pull` that changed the code

On each server, rebuild what changed and restart it (replicas, agents and placement stay):

```bash
scripts/rebuild-stages.sh                      # the stages that run on this site
scripts/rebuild-stages.sh --all                # all six (keep them current for a placement change)
scripts/rebuild-stages.sh timing-collector     # or any of gateway, timing-collector, local-controller
```

It checks at the end that the restarted stages run the worker of the checkout. For a
change to the manifests or `site.env`, use `scripts/setup-site.sh` instead.

### Before every run

1. **The tunnel**, on the edge (step 3): it does not survive a night.
2. **On each server**:
   ```bash
   scripts/prepare-run.sh             # --check: only the checks
   ```
   It checks the clock, that `/mnt/ramdisk` is a tmpfs with the video and the seeds (also as
   the pods see it), the pods, the disk, and the other site (the bridge from the edge, the
   gateway through the tunnel from the cloud). If a check fails it stops, says what to do and
   changes nothing. Otherwise it removes the pods that failed or were evicted, copies the
   logs of the previous run to `~/runs/unsaved-<time>/` if they were not empty, empties
   them (the controller keeps `pods.csv` open: emptied, not removed), removes the job
   directories and restarts the agents, so that every run starts from freshly started
   agents (they do not learn during a run: this does not change their decisions). It ends
   by printing the placement in force.
3. **The placement**, from the cloud VM, if it is not the one you want:
   ```bash
   .venv/bin/python scripts/placement.py publish placements/split-ffmpeg2.yaml --broker localhost:31883
   ```

### The run

In `tmux` on both servers, with the same name and minutes, the edge first:

```bash
scripts/monitor-run.sh <run> <minutes>          # on the edge
scripts/run-experiment.sh <run> <minutes>       # on the cloud VM, right after
```

`run-experiment.sh` writes `run.yaml`, starts the same monitor for the cloud and runs
Locust (installed the first time) on `localhost:38080`, the edge gateway through the tunnel.
Locust's profile lasts one hour: two 20-minute waves with no load in between, up to about
0.28 jobs/s, about 380 jobs; a shorter run takes its first minutes. Locust stops after the
given minutes even if jobs are still in the pipeline; both monitors keep watching 5 more
minutes, so that those jobs end up in the logs, then save the logs in
`~/runs/<run>/<site>/`. A monitor stopped with Ctrl-c saves them too. The edge monitor
prints a warning when the probes through the bridge are lost: the tunnel is down.

A single job by hand, to check the pipeline:

```bash
curl -X POST http://localhost:38080/workflow/invoke/videosearcher_workflow \
     -H 'Content-Type: application/json' -d '{"params": {}, "Async": false}'
```

### After the run

1. **On your machine**, when both monitors have saved their logs:
   ```bash
   scripts/fetch-run.sh <run>
   ```
   It copies the run into `results/<run>/` and runs `scripts/summarize_run.py` on it with the
   duration of `run.yaml`, writing the CSV files used by the notebook.

   The run lasts `--duration` seconds (default 3600) from its first job: only what ends
   inside it counts, and the jobs still in the pipeline at the end are reported apart. It
   prints the jobs sent, completed, failed and duplicated; per 5 minutes the job duration
   and the wait of every stage; the waits of the hops between the sites; the service time of
   every stage on every site; the decisions of the agents and the replicas that stayed not
   ready; the files each stage wrote (count and size, from the job's record). It ends with
   five checks (no failed job, no duplicate, every agent answered, replicas ready in time,
   every stage wrote its output) and exits with an error if one fails: then the run is to
   be looked at before being used. Every job processes the same video, so a stage should
   write the same files every time: an output is odd when it has another number of files
   than usual for that stage on that site, or a size more than 10% away from the median
   (`--output-tolerance`). The table shows the largest deviation of every stage: use it to
   choose a tighter tolerance once the spread of the real runs is known.

   With `--out` it writes the CSV files used by the notebook, times in seconds from the start
   of the run: `jobs.csv` (one row per job), `stages.csv` (one row per job and stage: site,
   pod, service time, wait, files and bytes written), `replicas.csv` (replicas of every stage on every site, every
   5 s) and `agents.csv` (every observation window: the state, what the agent received, its
   answer, the replicas applied).
2. **Plots and tables:** `analysis/run_analysis.ipynb`. The first time:
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
3. **Before the next run**, `scripts/prepare-run.sh` on each server again: it cleans up.

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
