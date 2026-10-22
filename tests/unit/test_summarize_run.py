import os
import sys

import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
for sub in ("common", "scripts"):
    sys.path.insert(0, os.path.join(ROOT, sub))

import summarize_run  # noqa: E402
from multisite import STAGES  # noqa: E402

START, END = 1000.0, 4600.0


def job(job_id, received, logged_at, sites=None, wait=0.01, topic="pipeline/edge/done", error=None):
    sites = sites or {s: "edge" for s in STAGES}
    stages, t = [], received
    for stage in STAGES:
        stages.append({"stage": stage, "site": sites[stage], "start": t + wait, "end": t + wait + 1, "wait": wait})
        t += wait + 1
    return {"job_id": job_id, "topic": topic, "logged_at": logged_at, "error": error,
            "timestamps": {"gateway_received": received}, "sites": {"gateway": "edge", **sites},
            "stages": stages}


def window(t, arrivals, window_s=120.0):
    return {"t": t, "site": "edge", "window_s": window_s, "lambda": arrivals / window_s, "stages": {}}


def test_only_what_ends_inside_the_run_is_counted():
    timing = [job("a", 1010, 1020), job("a", 1010, 1021),            # duplicate
              job("b", 1100, 1110, topic="pipeline/edge/dlq", error={"stage": "grep"}),
              job("c", 4590, 4650)]                                    # finished after the end
    observations = [window(1120, 3), window(4600, 1), window(4720, 0)]
    jobs = summarize_run.jobs_summary(timing, observations, START, END)
    assert jobs["sent"] == 4
    assert [r["job_id"] for r in jobs["completed"]] == ["a"]
    assert [r["job_id"] for r in jobs["failed"]] == ["b"]
    assert (jobs["duplicates"], jobs["after_end"], jobs["in_pipeline_at_end"]) == (1, 1, 2)


def test_buckets_group_jobs_by_arrival():
    completed = [job("a", 1010, 1020, wait=0.5), job("b", 1400, 1410, wait=2.5)]
    rows = summarize_run.per_bucket(completed, START, START + 600, 300)
    assert [(minute, n) for minute, n, *_ in rows] == [(0, 1), (5, 1)]
    assert rows[1][4]["deepspeech"] == pytest.approx(2.5)
    assert rows[0][2] == pytest.approx(6 * 1.5)


def test_hops_between_sites_are_split_from_local_ones():
    sites = {s: ("cloud" if s in ("deepspeech", "grep") else "edge") for s in STAGES}
    local, remote = summarize_run.hop_waits([job("a", 1010, 1020, sites=sites)])
    assert (len(local), len(remote)) == (5, 1)    # only ffmpeg2 (edge) -> deepspeech (cloud)


def test_agent_decisions_inside_the_run():
    agent = {"previous": 1, "action": 3, "applied": 3, "error": None}
    observations = [{"t": 1120, "site": "edge", "stages": {"ffmpeg2": {"agent": agent}, "grep": {}}},
                    {"t": 4700, "site": "edge", "stages": {"ffmpeg2": {"agent": agent}}}]
    decisions = summarize_run.agent_decisions(observations, START, END)
    assert decisions == {("edge", "ffmpeg2"): [(2.0, 1, 3, 3, None)]}


def test_not_ready_stretches_while_placed():
    def row(t, desired, ready, placed="1"):
        return {"timestamp": str(t), "site": "edge", "stage": "ffmpeg2", "placed_here": placed,
                "desired": str(desired), "ready": str(ready)}
    pods = [row(1000, 2, 2), row(1005, 3, 2), row(1010, 3, 2), row(1015, 3, 3),
            row(1020, 4, 3), row(1025, 4, 4), row(1030, 0, 0, placed="0")]
    assert summarize_run.not_ready_stretches(pods, START, END) == {("edge", "ffmpeg2"): (10.0, 15.0)}
