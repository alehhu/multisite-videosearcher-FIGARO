import json
import os
import socket
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
for sub in ("common", "scripts"):
    sys.path.insert(0, os.path.join(ROOT, sub))

import profile_stages  # noqa: E402
from multisite import GROUPS, STAGES  # noqa: E402

# Seconds of each stage in a job run on the edge (same shape as a real gateway reply).
DURATIONS = {"ffmpeg0": 0.4, "librosa": 2.1, "ffmpeg1": 0.3, "ffmpeg2": 16.7, "deepspeech": 20.4, "grep": 0.02}


def reply_for(durations, site="edge", start=1000.0, hop=0.001):
    ts, t = {"gateway_received": start, "gateway_published": start}, start
    for stage in STAGES:
        group = GROUPS[stage]
        t += hop
        ts[f"{group}_start"] = t
        t += durations[stage]
        ts[f"{group}_end"] = t
    total = t - start + 0.05
    return {"status": "success", "job_id": "job-1",
            "result": {"timestamps": ts, "sites": {s: site for s in STAGES}},
            "Reports": {"Gateway": {"Duration": total}}}


def test_parse_job_takes_end_minus_start_of_each_stage():
    job = profile_stages.parse_job(reply_for(DURATIONS))
    for stage, seconds in DURATIONS.items():
        assert job["stages"][stage] == ("edge", pytest.approx(seconds))
    assert job["overhead"] == pytest.approx(6 * 0.001 + 0.05)


def test_summary_over_jobs():
    jobs = [profile_stages.parse_job(reply_for({**DURATIONS, "ffmpeg2": s})) for s in (15.0, 17.0)]
    rows = {row[0]: row for row in profile_stages.summary_rows(jobs)}
    name, site, n, mean, std, low, high = rows["ffmpeg2"]
    assert (site, n, low, high) == ("edge", 2, pytest.approx(15.0), pytest.approx(17.0))
    assert mean == pytest.approx(16.0)
    assert set(rows) == set(STAGES) | {"job total", "overhead"}


class FakeGateway(BaseHTTPRequestHandler):
    replies = []

    def do_POST(self):
        self.rfile.read(int(self.headers["Content-Length"]))
        code, body = self.replies.pop(0)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def log_message(self, *args):
        pass


def test_end_to_end_with_warmup_and_a_failed_job(tmp_path, monkeypatch, capsys):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    FakeGateway.replies = [
        (200, reply_for({**DURATIONS, "librosa": 9.0})),   # warmup: slow, not counted
        (200, reply_for(DURATIONS)),
        (500, {"status": "error", "error": {"stage": "grep", "message": "boom"}}),
        (200, reply_for(DURATIONS)),
    ]
    server = HTTPServer(("127.0.0.1", port), FakeGateway)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    out = tmp_path / "jobs.csv"
    monkeypatch.setattr(sys, "argv", ["profile_stages.py", "--gateway", f"127.0.0.1:{port}",
                                      "-n", "3", "--csv", str(out)])
    try:
        assert profile_stages.main() == 1   # one job failed
    finally:
        server.shutdown()
    printed = capsys.readouterr().out
    assert "not counted" in printed and "FAILED" in printed
    librosa = next(line for line in printed.splitlines() if line.startswith("librosa"))
    assert " 2 " in librosa and "9.000" not in librosa   # two measured jobs, warmup left out
    rows = out.read_text().splitlines()
    assert len(rows) == 1 + 2 * (len(STAGES) + 1)
