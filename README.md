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
- Each site logs per-job timings, pod counts, placement changes and the state used by
  the scaling agents.

Tested locally with `tests/integration`; not yet run on the two servers.

## Layout

```text
common/            stage order, topics, placement table and the MQTT worker
functions/         the 6 pipeline stages
gateway/           HTTP -> MQTT entry point (edge)
controller/        per-site controller
monitoring/        timing collector
deploy/            Kubernetes manifests (base + edge/cloud overlays), broker bridge configs
placements/        example placement tables
scripts/           build, deploy, placement changes, seeds, tunnel, link probe
load/locust/       workload generator
tests/             unit tests and a two-broker integration test
data/video.mp4     benchmark input (30 s clip)
```

## Usage

See [docs/MULTISITE.md](docs/MULTISITE.md).

## Origin

The pipeline stages, gateway, timing collector and workload generator come from
[mqtt-faas-hpa-baseline](https://github.com/alehhu/mqtt-faas-hpa-baseline) (commit `7b8e7a9`).
