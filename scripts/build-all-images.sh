#!/bin/bash
# Build all Docker images. Run from the repository root.
set -euo pipefail

STAGES=("ffmpeg0" "librosa" "ffmpeg1" "ffmpeg2" "deepspeech" "grep")

for stage in "${STAGES[@]}"; do
    echo "Building $stage:latest"
    docker build -t "$stage:latest" -f "functions/$stage/Dockerfile" .
done

echo "Building gateway:latest"
docker build -t gateway:latest -f gateway/Dockerfile gateway/

echo "Building timing-collector:latest"
docker build -t timing-collector:latest -f monitoring/timing_collector/Dockerfile monitoring/timing_collector/

echo "All images built."
