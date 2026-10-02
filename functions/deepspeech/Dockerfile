FROM registry.gitlab.polimi.it/ai-sprint/toscarizer/ai-sprint-base
# FROM python:3.9-slim

# # Install ffmpeg and python deps
# RUN apt update && apt install -y ffmpeg && \
#     pip install --no-cache-dir torch openai-whisper


RUN mkdir /opt/videosearcher
RUN mkdir /mnt/ramdisk

WORKDIR /opt/videosearcher

# Copy requirements files
COPY ./functions/deepspeech/requirements.* /opt/videosearcher/

# Instal system requirements first
RUN touch requirements.sys && \
    apt update && \
    xargs apt install -y --no-install-recommends < requirements.sys  && \
    apt-get autoremove -y && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && rm -rf ~/.cache/

# Update pip and install python requirements
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Download DeepSpeech model artifacts used at runtime by function.py
ARG DEEPSPEECH_VERSION=0.9.3
RUN set -e; \
    wget --tries=3 --timeout=30 --show-progress -O /opt/videosearcher/deepspeech-${DEEPSPEECH_VERSION}-models.pbmm \
      "https://github.com/mozilla/DeepSpeech/releases/download/v${DEEPSPEECH_VERSION}/deepspeech-${DEEPSPEECH_VERSION}-models.pbmm"; \
    wget --tries=3 --timeout=30 --show-progress -O /opt/videosearcher/deepspeech-${DEEPSPEECH_VERSION}-models.scorer \
      "https://github.com/mozilla/DeepSpeech/releases/download/v${DEEPSPEECH_VERSION}/deepspeech-${DEEPSPEECH_VERSION}-models.scorer"

# Copy application and onnx models
COPY ./functions/deepspeech/ /opt/videosearcher/
COPY ./common/*.py /opt/videosearcher/

CMD ["python3", "/opt/videosearcher/function.py"]
