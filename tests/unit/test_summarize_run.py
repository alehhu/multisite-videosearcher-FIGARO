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
        stages.append({"stage": stage, "site": sites[stage], "start": t + wait, "end": t + wait + 1,
                       "duration": 1, "wait": wait})
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


def test_service_times_by_stage_and_site():
    sites = {s: ("cloud" if s == "grep" else "edge") for s in STAGES}
    times = summarize_run.service_times([job("a", 1010, 1020), job("b", 1020, 1030, sites=sites)])
    assert times[("grep", "edge")] == [1] and times[("grep", "cloud")] == [1]
    assert len(times[("deepspeech", "edge")]) == 2


def test_export_writes_the_csv_files(tmp_path):
    import csv
    agent = {"previous": 1, "action": 3, "applied": 3, "error": None}
    logs = {
        "timing": [job("a", 1010, 1020), job("a", 1010, 1020), job("b", 4590, 4650)],
        "observations": [{"t": 1120, "site": "edge", "window_s": 120, "placement_version": 1,
                          "stages": {"ffmpeg2": {"raw": {"pressure": 0.5}, "observation": {"pressure": 0.17},
                                                 "agent": agent}}}],
        "pods": [{"timestamp": "1005", "site": "edge", "stage": "ffmpeg2", "placed_here": "1", "draining": "0",
                  "desired": "2", "ready": "1", "alive": "2", "terminating": "0", "hpa": "0"}],
    }
    written = summarize_run.export(str(tmp_path), logs, START, END)
    assert written == {"jobs.csv": 2, "stages.csv": 12, "replicas.csv": 1, "agents.csv": 1}
    read = lambda name: list(csv.DictReader(open(tmp_path / name)))
    jobs = read("jobs.csv")
    assert [(j["job_id"], j["arrival"], j["ended_in_run"]) for j in jobs] == [("a", "10.0", "1"), ("b", "3590.0", "0")]
    assert float(jobs[0]["duration"]) == pytest.approx(6 * 1.01)
    first = read("stages.csv")[0]
    assert (first["stage"], first["site"], first["service_time"], first["wait"]) == ("ffmpeg0", "edge", "1", "0.01")
    assert read("replicas.csv")[0]["t"] == "5.0"
    row = read("agents.csv")[0]
    assert (row["t"], row["pressure"], row["obs_pressure"], row["applied"]) == ("120.0", "0.5", "0.17", "3")
