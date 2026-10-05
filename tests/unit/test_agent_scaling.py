import os
import socket
import sys

import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
for sub in ("common", "controller", os.path.join("tests", "integration")):
    sys.path.insert(0, os.path.join(ROOT, sub))

from fake_agent import FakeAgents  # noqa: E402  (answers 2, 3, 1, ... per stage)
from local_controller import LocalController  # noqa: E402
from multisite import STAGES, Placement  # noqa: E402

OBSERVATION = {"n_instances": 0.1, "utilization": 0.5, "pressure": 0.2,
               "queue_length_dominant": 0.0, "workload": 0.4}


class StubBackend:
    def __init__(self, replicas=1):
        self.replicas = replicas
        self.scaled = []

    def desired(self, stage):
        return self.replicas

    def scale(self, stage, replicas):
        self.scaled.append((stage, replicas))
        self.replicas = replicas


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def agents():
    server = FakeAgents(free_port())
    yield server
    server.stop()


def make_controller(tmp_path, agent_cfg, replicas=1):
    config = {"agent_timeout_s": 1,
              "stages": {s: {"replicas": 1} for s in STAGES}}
    config["stages"]["ffmpeg2"]["agent"] = agent_cfg
    backend = StubBackend(replicas)
    controller = LocalController("edge", config, backend, log_dir=str(tmp_path))
    controller.current = Placement(1, "edge", {s: "edge" for s in STAGES})
    return controller, backend


def test_applies_the_agent_answer(tmp_path, agents):
    controller, backend = make_controller(tmp_path, {"url": agents.url("ffmpeg2"), "min": 1, "max": 5})
    result = controller._ask_agent("ffmpeg2", OBSERVATION)
    assert result == {"action": 2, "applied": 2, "previous": 1, "error": None}
    assert backend.scaled == [("ffmpeg2", 2)]
    assert agents.requests[0][1] == {"observation": OBSERVATION}


def test_answer_is_clamped_to_the_configured_range(tmp_path, agents):
    controller, backend = make_controller(tmp_path, {"url": agents.url("ffmpeg2"), "min": 3, "max": 5})
    result = controller._ask_agent("ffmpeg2", OBSERVATION)   # agent says 2, minimum is 3
    assert (result["action"], result["applied"]) == (2, 3)
    assert backend.scaled == [("ffmpeg2", 3)]


def test_unchanged_answer_does_not_rescale(tmp_path, agents):
    controller, backend = make_controller(tmp_path, {"url": agents.url("ffmpeg2")}, replicas=2)
    result = controller._ask_agent("ffmpeg2", OBSERVATION)   # agent says 2, already 2
    assert result["applied"] == 2
    assert backend.scaled == []


def test_unreachable_agent_keeps_the_replicas(tmp_path):
    controller, backend = make_controller(tmp_path, {"url": f"http://127.0.0.1:{free_port()}"}, replicas=3)
    result = controller._ask_agent("ffmpeg2", OBSERVATION)
    assert result["error"] and result["applied"] is None and result["previous"] == 3
    assert backend.scaled == []


def test_draining_stage_is_not_rescaled(tmp_path, agents):
    controller, backend = make_controller(tmp_path, {"url": agents.url("ffmpeg2")})
    controller.draining["ffmpeg2"] = {"since": 0, "version": 2}
    result = controller._ask_agent("ffmpeg2", OBSERVATION)
    assert result["action"] == 2 and result["applied"] is None
    assert backend.scaled == []


def test_agent_and_hpa_together_are_rejected(tmp_path):
    config = {"stages": {s: {"replicas": 1} for s in STAGES}}
    config["stages"]["ffmpeg2"].update(agent={"url": "http://x"}, hpa={"min": 1, "max": 2, "cpu_target": 60})
    with pytest.raises(ValueError):
        LocalController("edge", config, StubBackend(), log_dir=str(tmp_path))
