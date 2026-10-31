import os
import subprocess

SCRIPT = os.path.join(os.path.dirname(__file__), "..", "..", "scripts", "clean-ramdisk.sh")
JOBS = ["job-abc", "1793448252.123513"]
KEPT = ["seed", "seed-capture", "test-1"]


def make(tmp_path):
    for name in JOBS + KEPT:
        (tmp_path / name).mkdir()
    (tmp_path / "video.mp4").write_text("video")
    (tmp_path / "12345.txt").write_text("not a directory")


def run(tmp_path, *args):
    return subprocess.run([SCRIPT, "--dir", str(tmp_path), *args], capture_output=True, text=True)


def test_dry_run_removes_nothing(tmp_path):
    make(tmp_path)
    before = sorted(os.listdir(tmp_path))
    result = run(tmp_path)
    assert result.returncode == 0 and "2 job directories" in result.stdout
    assert sorted(os.listdir(tmp_path)) == before


def test_delete_removes_the_jobs_of_the_gateway_and_of_locust(tmp_path):
    make(tmp_path)
    assert run(tmp_path, "--delete").returncode == 0
    assert sorted(os.listdir(tmp_path)) == sorted(KEPT + ["video.mp4", "12345.txt"])


def test_a_pattern_replaces_the_default_names(tmp_path):
    make(tmp_path)
    assert run(tmp_path, "--pattern", "test-*", "--delete").returncode == 0
    assert "test-1" not in os.listdir(tmp_path) and all(j in os.listdir(tmp_path) for j in JOBS)


def test_patterns_matching_the_seeds_are_refused(tmp_path):
    make(tmp_path)
    for pattern in ("", "*", "seed", "seed*"):
        assert run(tmp_path, "--pattern", pattern, "--delete").returncode != 0
    assert "seed" in os.listdir(tmp_path)
