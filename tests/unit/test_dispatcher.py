import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "common"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "dispatcher"))
from dispatcher import Board  # noqa: E402

STAGE = "deepspeech"


def job(name):
    return {"dir": name, "timestamps": {}}


def assigned(board):
    return [(worker, params["dir"]) for worker, params, _ in board.assignments(STAGE)]


def test_jobs_wait_until_a_worker_is_idle():
    b = Board()
    b.add_job(STAGE, job("j1"), token=(1, 1))
    b.add_job(STAGE, job("j2"), token=(2, 1))
    assert assigned(b) == []
    b.worker_state(STAGE, "w1", "idle")
    assert assigned(b) == [("w1", "j1")]
    assert assigned(b) == []                    # w1 is busy now
    b.worker_state(STAGE, "w1", "idle", last_job="j1")
    assert assigned(b) == [("w1", "j2")]


def test_a_new_worker_takes_from_the_backlog():
    # The point of the dispatcher: a worker started late serves the jobs already waiting.
    b = Board()
    b.worker_state(STAGE, "old", "idle")
    for i in range(5):
        b.add_job(STAGE, job(f"j{i}"))
    assert assigned(b) == [("old", "j0")]
    b.worker_state(STAGE, "new", "idle")
    assert assigned(b) == [("new", "j1")]
    b.worker_state(STAGE, "new", "idle", last_job="j1")
    b.worker_state(STAGE, "old", "idle", last_job="j0")
    assert assigned(b) == [("new", "j2"), ("old", "j3")]


def test_jobs_go_in_arrival_order_to_workers_in_idle_order():
    b = Board()
    for w in ("a", "b", "c"):
        b.worker_state(STAGE, w, "idle")
    for i in range(2):
        b.add_job(STAGE, job(f"j{i}"))
    assert assigned(b) == [("a", "j0"), ("b", "j1")]


def test_stages_are_independent():
    b = Board()
    b.worker_state("ffmpeg2", "w", "idle")
    b.add_job(STAGE, job("j1"))
    assert b.assignments("ffmpeg2") == [] and assigned(b) == []


def test_stale_idle_does_not_free_a_busy_worker():
    # Idle sent (e.g. answering a hello) before the job sent to it arrived.
    b = Board()
    b.worker_state(STAGE, "w", "idle")
    b.add_job(STAGE, job("j1"))
    b.add_job(STAGE, job("j2"))
    assert assigned(b) == [("w", "j1")]
    assert b.worker_state(STAGE, "w", "idle", last_job=None) == "stale"
    assert assigned(b) == []


def test_duplicate_idle_counts_once():
    b = Board()
    b.worker_state(STAGE, "w", "idle")
    b.worker_state(STAGE, "w", "idle")
    b.add_job(STAGE, job("j1"))
    b.add_job(STAGE, job("j2"))
    assert assigned(b) == [("w", "j1")]


def test_lost_worker_job_is_published_again():
    b = Board()
    b.worker_state(STAGE, "w1", "idle")
    b.add_job(STAGE, job("j1"))
    assert assigned(b) == [("w1", "j1")]
    b.worker_state(STAGE, "w1", "lost")
    [(stage, params)] = b.take_requeued()
    assert (stage, params["dir"], params["requeued"]) == (STAGE, "j1", {STAGE: 1})
    assert b.take_requeued() == [] and not b.busy[STAGE]


def test_leaving_after_its_last_job_requeues_nothing():
    b = Board()
    b.worker_state(STAGE, "w", "idle")
    b.add_job(STAGE, job("j1"))
    assigned(b)
    assert b.worker_state(STAGE, "w", "leaving", last_job="j1") == "leaving"
    assert b.take_requeued() == [] and not b.busy[STAGE]


def test_leaving_before_taking_the_job_sent_requeues_it():
    b = Board()
    b.worker_state(STAGE, "w", "idle")
    b.add_job(STAGE, job("j1"))
    assigned(b)                                     # sent, but w got SIGTERM first
    b.worker_state(STAGE, "w", "leaving", last_job=None)
    assert [p["dir"] for _, p in b.take_requeued()] == ["j1"]


def test_leaving_idle_worker_gets_nothing_more():
    b = Board()
    b.worker_state(STAGE, "w", "idle")
    b.worker_state(STAGE, "w", "leaving")
    b.add_job(STAGE, job("j1"))
    assert assigned(b) == []


def test_reconnection_drops_the_waiting_jobs():
    # They are not acknowledged yet: the broker sends them again.
    b = Board()
    b.add_job(STAGE, job("j1"), token=(7, 1))
    b.add_job("ffmpeg2", job("j2"), token=(8, 1))
    assert b.drop_waiting() == 2
    assert not any(b.waiting.values())
