"""The DQN state is computed as in Pozzani's controller (rlagent.py on Serverledge gauges)."""

import os
import sys

import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
for sub in ("common", "controller"):
    sys.path.insert(0, os.path.join(ROOT, sub))

from local_controller import LocalController  # noqa: E402
from multisite import GROUPS, STAGES, Placement  # noqa: E402

STAGE = "ffmpeg2"


class StubBackend:
    def desired(self, stage):
        return 1

    def site_utilization(self):
        return {}


def make_controller(tmp_path, threshold=10.0):
    config = {"stages": {s: {"replicas": 1} for s in STAGES}}
    config["stages"][STAGE].update(res_time_threshold=threshold, agent={"url": "http://127.0.0.1:9"})
    controller = LocalController("edge", config, StubBackend(), log_dir=str(tmp_path))
    controller.current = Placement(1, "edge", {s: "edge" for s in STAGES})
    return controller


def finish_job(controller, response_time, service_time, end=1000.0):
    previous = GROUPS[STAGES[STAGES.index(STAGE) - 1]]
    controller._record_completion(STAGE, {
        f"{previous}_published": end - response_time,
        f"{GROUPS[STAGE]}_start": end - service_time,
        f"{GROUPS[STAGE]}_end": end,
    })


def sample(controller, n, now=1000.0, ready=1):
    for _ in range(n):
        controller.window[STAGE].agent_samples.append(controller._agent_sample(STAGE, now, ready))


def observe(controller):
    return controller._stage_observation(STAGE, controller.window[STAGE], 120.0, {})


def test_times_are_those_of_the_last_job_weighted_by_time(tmp_path):
    controller = make_controller(tmp_path)
    finish_job(controller, response_time=10, service_time=4)
    sample(controller, 3)
    finish_job(controller, response_time=20, service_time=4)
    sample(controller, 1)
    result = observe(controller)
    assert result["raw"]["response_time"] == pytest.approx((3 * 10 + 20) / 4)
    assert result["per_job"]["response_time"] == pytest.approx(15)
    assert result["raw"]["pressure"] == pytest.approx(1.25)


def test_times_carry_over_a_window_without_jobs(tmp_path):
    controller = make_controller(tmp_path)
    finish_job(controller, response_time=12, service_time=6)
    controller._observe()
    sample(controller, 2)
    result = observe(controller)
    assert result["raw"]["completions"] == 0
    assert (result["raw"]["response_time"], result["raw"]["service_time"]) == (12, 6)
    assert result["raw"]["queue_length_dominant"] == pytest.approx(1.0)


def test_workload_counts_the_last_six_buckets_of_ten_seconds(tmp_path):
    controller = make_controller(tmp_path)
    controller.recent_arrivals[STAGE] = [940.0, 949.9, 950.0, 975.0, 1009.0]
    s = controller._agent_sample(STAGE, 1009.5, 1)   # buckets 95..100
    assert s["workload"] == pytest.approx(3 / 60)
    assert controller.recent_arrivals[STAGE] == [950.0, 975.0, 1009.0]


def test_ratios_are_averaged_per_sample(tmp_path):
    controller = make_controller(tmp_path)
    finish_job(controller, response_time=10, service_time=5)
    sample(controller, 1)
    finish_job(controller, response_time=30, service_time=10)
    sample(controller, 1)
    result = observe(controller)
    assert result["raw"]["queue_length_dominant"] == pytest.approx((1 + 2) / 2)


def test_utilization_uses_the_instances_of_each_sample(tmp_path):
    controller = make_controller(tmp_path)
    finish_job(controller, response_time=20, service_time=20)
    controller.recent_arrivals[STAGE] = [1000.0] * 6   # 0.1 jobs/s
    sample(controller, 1, ready=1)
    sample(controller, 1, ready=4)
    result = observe(controller)
    assert result["raw"]["theoretical_utilization"] == pytest.approx((1.0 + 0.5) / 2)
    assert result["observation"]["utilization"] == pytest.approx(0.75)


def test_no_agent_request_without_samples(tmp_path):
    controller = make_controller(tmp_path)
    asked = []
    controller._ask_agent = lambda stage, obs: asked.append(stage)
    controller._observe()
    assert asked == []
    sample(controller, 1)
    controller._observe()
    assert asked == [STAGE]
