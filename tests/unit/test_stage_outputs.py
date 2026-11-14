import os
import sys
import time

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
sys.path.insert(0, os.path.join(ROOT, "common"))

from pipeline_worker import files_in, written_since  # noqa: E402


def test_new_and_changed_files_are_reported_removed_and_untouched_ones_are_not(tmp_path):
    job = tmp_path / "job-1"
    (job / "sub").mkdir(parents=True)
    (job / "input.tar.gz").write_bytes(b"x" * 10)      # removed by the stage
    (job / "kept.txt").write_bytes(b"k" * 3)           # left alone
    (job / "rewritten.txt").write_bytes(b"r")          # changed by the stage
    before = files_in(job)
    time.sleep(0.01)
    (job / "input.tar.gz").unlink()
    (job / "rewritten.txt").write_bytes(b"rr")
    (job / "out_0.tar.gz").write_bytes(b"o" * 7)
    (job / "sub" / "out_1.tar.gz").write_bytes(b"o" * 5)
    assert written_since(before, files_in(job)) == {
        "out_0.tar.gz": 7, "rewritten.txt": 2, os.path.join("sub", "out_1.tar.gz"): 5}


def test_missing_directory():
    assert files_in("/nonexistent/job-0") == {}
    assert written_since({}, {}) == {}
