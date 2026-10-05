"""A stage that moves to the other site starts there with the replicas it had before."""

import os
import sys

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
for sub in ("common", "controller"):
    sys.path.insert(0, os.path.join(ROOT, sub))

from local_controller import LocalController  # noqa: E402
from multisite import STAGES, Placement  # noqa: E402

ALL_CLOUD = {s: "cloud" for s in STAGES}
FFMPEG2_TO_EDGE = {**ALL_CLOUD, "ffmpeg2": "edge"}


class StubBackend:
    def __init__(self):
        self.replicas = {s: 0 for s in STAGES}
        self.ready_replicas = {s: 0 for s in STAGES}

    def desired(self, stage):
        return self.replicas[stage]

    def ready(self, stage):
        return self.ready_replicas[stage]

    def scale(self, stage, replicas):
        self.replicas[stage] = replicas

    def ensure_hpa(self, stage, hpa):
        pass

    def delete_hpa(self, stage):
        pass


class StubClient:
    def __init__(self):
        self.published = []

    def publish(self, topic, payload, **kwargs):
        self.published.append((topic, payload))


def make_controller(tmp_path, stage_cfg=None):
    config = {"stages": {s: {"replicas": 1} for s in STAGES}}
    config["stages"]["ffmpeg2"].update(stage_cfg or {})
    backend = StubBackend()
    controller = LocalController("edge", config, backend, log_dir=str(tmp_path))
    controller.client = StubClient()
    controller.current = Placement(1, "edge", ALL_CLOUD)
    return controller, backend


def test_placement_carries_replicas_only_when_given():
    plain = Placement(1, "edge", ALL_CLOUD)
    assert "replicas" not in plain.to_dict()
    moved = Placement.from_json(Placement(2, "edge", FFMPEG2_TO_EDGE, {"ffmpeg2": 3}).to_json())
    assert moved.replicas == {"ffmpeg2": 3}


def test_incoming_stage_starts_with_the_replicas_it_had(tmp_path):
    controller, backend = make_controller(tmp_path)
    controller._on_pending(Placement(2, "edge", FFMPEG2_TO_EDGE, {"ffmpeg2": 3}))
    assert backend.replicas["ffmpeg2"] == 3


def test_without_replicas_the_configured_number_is_used(tmp_path):
    controller, backend = make_controller(tmp_path)
    controller._on_pending(Placement(2, "edge", FFMPEG2_TO_EDGE))
    assert backend.replicas["ffmpeg2"] == 1


def test_carried_replicas_respect_the_agent_range(tmp_path):
    controller, backend = make_controller(tmp_path, {"agent": {"url": "http://unused", "min": 1, "max": 2}})
    controller._on_pending(Placement(2, "edge", FFMPEG2_TO_EDGE, {"ffmpeg2": 5}))
    assert backend.replicas["ffmpeg2"] == 2


def test_the_move_is_acked_only_when_all_replicas_are_ready(tmp_path):
    controller, backend = make_controller(tmp_path)
    controller._on_pending(Placement(2, "edge", FFMPEG2_TO_EDGE, {"ffmpeg2": 3}))
    backend.ready_replicas["ffmpeg2"] = 2
    controller._check_ready()
    assert controller.acked_version == 0
    backend.ready_replicas["ffmpeg2"] = 3
    controller._check_ready()
    assert controller.acked_version == 2
