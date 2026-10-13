# multisite-videosearcher-FIGARO

The VideoSearcher pipeline (`ffmpeg0 → librosa → ffmpeg1 → ffmpeg2 → deepspeech → grep`)
running across two sites, an edge and a cloud, connected over MQTT. Each stage can be
placed on either site, and the placement can change while jobs are running.

```
client ──► EDGE (Modena) ◄──MQTT bridge──► CLOUD (Milano)
```

- Each site has its own k3s cluster and Mosquitto broker; the two brokers are bridged.
- The placement (which site runs each stage) is a retained MQTT message. A controller on
  each site starts the stages assigned to it and drains the ones that moved away.
- Files are not transferred between sites: a job arriving from the other site reads its
  input from a local seed copy, so the experiment measures latency, not bandwidth.
- Each stage is scaled by a reinforcement-learning agent running on the same site: at
  every observation window the controller sends it the stage's state and applies the
  number of instances it returns. The agents can be switched off (fixed replicas).
- Each site logs per-job timings, pod counts, placement changes, and the observations
  and decisions of the scaling agents.

Tested locally with `tests/integration`; not yet run on the two servers.

## Layout

```text
common/            stage order, topics, placement table and the MQTT worker
functions/         the 6 pipeline stages
gateway/           HTTP -> MQTT entry point (edge)
controller/        per-site controller
monitoring/        timing collector
deploy/            Kubernetes manifests (base, scaling agents, site overlays), broker bridge configs
placements/        example placement tables
scripts/           site setup, build, deploy, placement changes, seeds, tunnel, link probe
load/locust/       workload generator
tests/             unit tests and a two-broker integration test
data/video.mp4     benchmark input (30 s clip)
data/seed/         input of every stage, once captured
site.env           settings of the server (edited locally on each one)
```

## Usage

On each server, from a clone of this repository:

```bash
$EDITOR site.env                  # which site this is
scripts/setup-site.sh
```

Then publish a placement. The whole procedure is in [docs/MULTISITE.md](docs/MULTISITE.md).

## Origin

The pipeline stages, gateway, timing collector and workload generator come from
[mqtt-faas-hpa-baseline](https://github.com/alehhu/mqtt-faas-hpa-baseline) (commit `7b8e7a9`).
The scaling agents are the DQN agents of
[ServerlessAutoscalingWithRL](https://github.com/sampozz/ServerlessAutoscalingWithRL)
(commit `0414a04`), built unchanged by `scripts/build-agent-image.sh`.
