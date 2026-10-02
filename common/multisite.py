"""Shared definitions for the multi-site pipeline: stage order, MQTT topics and placement.

Used by the pipeline workers, the gateway, the timing collector, the local controller
and the placement coordinator, so that every component agrees on names and topics.
"""

import json
import os
import time

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties


STAGES = ["ffmpeg0", "librosa", "ffmpeg1", "ffmpeg2", "deepspeech", "grep"]

# Timestamp prefixes inherited from the single-site baseline (groupA_start, ...).
GROUPS = {
    "ffmpeg0": "groupA",
    "librosa": "groupB",
    "ffmpeg1": "groupC",
    "ffmpeg2": "groupD",
    "deepspeech": "groupE",
    "grep": "groupF",
}

SITES = ["edge", "cloud"]

# Placement table: retained, published by the coordinator once every site has acked.
PLACEMENT_TOPIC = "control/placement/current"
# Next placement announced to the controllers, so they can start the new replicas first.
PLACEMENT_PENDING_TOPIC = "control/placement/pending"
# control/status/<site>: retained status of each local controller, e.g.
# {"acked_version": N, "current_version": M, "draining": [...]}. acked_version = N means the
# site has started (and seen ready) every stage that placement N moves onto it.
STATUS_TOPIC_PREFIX = "control/status"
# observations/<site>: periodic agent-ready metrics published by each local controller.
OBSERVATIONS_TOPIC_PREFIX = "observations"


def stage_topic(site, stage):
    """Topic where `stage` running on `site` receives its jobs."""
    return f"pipeline/{site}/{stage}"


def done_topic(site):
    return f"pipeline/{site}/done"


def dlq_topic(site):
    return f"pipeline/{site}/dlq"


def status_topic(site):
    return f"{STATUS_TOPIC_PREFIX}/{site}"


def observations_topic(site):
    return f"{OBSERVATIONS_TOPIC_PREFIX}/{site}"


def next_stage(stage):
    """Stage after `stage`, or None for the last one."""
    i = STAGES.index(stage)
    return STAGES[i + 1] if i + 1 < len(STAGES) else None


def previous_stage(stage):
    i = STAGES.index(stage)
    return STAGES[i - 1] if i > 0 else None


def parse_topic(topic):
    """pipeline/<site>/<target> -> (site, target), else None. target is a stage, 'done' or 'dlq'."""
    parts = topic.split("/")
    if len(parts) == 3 and parts[0] == "pipeline":
        return parts[1], parts[2]
    return None


class Placement:
    """Which site runs each stage, plus where the gateway lives (it receives done/dlq)."""

    def __init__(self, version, gateway, stages):
        self.version = int(version)
        self.gateway = gateway
        self.stages = dict(stages)
        self.validate()

    def validate(self):
        if self.gateway not in SITES:
            raise ValueError(f"gateway site must be one of {SITES}, got {self.gateway!r}")
        missing = [s for s in STAGES if s not in self.stages]
        if missing:
            raise ValueError(f"placement is missing stages: {missing}")
        for stage, site in self.stages.items():
            if stage not in STAGES:
                raise ValueError(f"unknown stage {stage!r}")
            if site not in SITES:
                raise ValueError(f"stage {stage} placed on unknown site {site!r}")

    def site_of(self, stage):
        return self.stages[stage]

    def stages_on(self, site):
        return [s for s in STAGES if self.stages[s] == site]

    def destination(self, stage):
        """(site, topic) where the output of `stage` must be published."""
        nxt = next_stage(stage)
        if nxt is None:
            return self.gateway, done_topic(self.gateway)
        site = self.stages[nxt]
        return site, stage_topic(site, nxt)

    def entry(self):
        """(site, topic) where the gateway publishes new jobs."""
        site = self.stages[STAGES[0]]
        return site, stage_topic(site, STAGES[0])

    def to_dict(self):
        return {"version": self.version, "gateway": self.gateway, "stages": self.stages}

    def to_json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data):
        return cls(data["version"], data["gateway"], data["stages"])

    @classmethod
    def from_json(cls, payload):
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode()
        return cls.from_dict(json.loads(payload))


def make_client(client_id="", manual_ack=False):
    """MQTT v5 client with automatic reconnection."""
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv5,
        manual_ack=manual_ack,
    )
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    return client


def connect(client, host=None, port=None, receive_maximum=None, retry_seconds=5):
    """Connect, retrying until the broker is reachable.

    receive_maximum=1 makes the broker hand the client one unacknowledged message at a
    time, so queued jobs wait in the broker instead of piling up inside a busy worker.
    """
    host = host or os.getenv("MQTT_BROKER", "mosquitto-service")
    port = int(port or os.getenv("MQTT_PORT", "1883"))
    properties = None
    if receive_maximum is not None:
        properties = Properties(PacketTypes.CONNECT)
        properties.ReceiveMaximum = receive_maximum
    while True:
        try:
            client.connect(host, port, properties=properties)
            return
        except OSError as exc:
            print(f"[multisite] MQTT {host}:{port} unreachable ({exc}), retrying in {retry_seconds}s", flush=True)
            time.sleep(retry_seconds)
