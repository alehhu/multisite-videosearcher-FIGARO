# multisite-videosearcher-FIGARO

The VideoSearcher pipeline (`ffmpeg0 → librosa → ffmpeg1 → ffmpeg2 → deepspeech → grep`)
over MQTT, as the starting point for running it across two sites (an edge and a cloud).

## Layout

```text
functions/     the 6 pipeline stages and the shared MQTT worker
gateway/       HTTP -> MQTT entry point
monitoring/    timing collector, pod logger
deploy/k8s/    Kubernetes manifests
load/locust/   workload generator
scripts/       image build
data/video.mp4 benchmark input (30 s clip)
```

## Origin

The pipeline stages, gateway, timing collector and workload generator come from
[mqtt-faas-hpa-baseline](https://github.com/alehhu/mqtt-faas-hpa-baseline) (commit `7b8e7a9`).
