import os
import sys
import threading

import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
for sub in ("common", "scripts"):
    sys.path.insert(0, os.path.join(ROOT, sub))

from multisite import STAGES  # noqa: E402
from placement import Coordinator  # noqa: E402


class StubClient:
    """Records what is published and lets the test answer as the sites would."""

    def __init__(self, coordinator, acking):
        self.coordinator = coordinator
        self.acking = acking
        self.published = []

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append(topic)
        version = len(self.published)
        with self.coordinator.changed:
            for site in self.acking:
                self.coordinator.status[site] = {"site": site, "acked_version": version}
        return self

    def wait_for_publish(self):
        pass


def make_coordinator(acking):
    coordinator = Coordinator.__new__(Coordinator)  # no broker
    coordinator.current = coordinator.pending = None
    coordinator.status = {}
    coordinator.changed = threading.Condition()
    coordinator.client = StubClient(coordinator, acking)
    return coordinator


ALL_EDGE = {stage: "edge" for stage in STAGES}


def test_single_site_does_not_wait_for_the_other():
    coordinator = make_coordinator(acking=["edge"])
    placement = coordinator.publish(ALL_EDGE, "edge", timeout=2, sites=["edge"])
    assert placement.version == 1
    assert len(coordinator.client.published) == 2  # pending, then current


def test_both_sites_are_waited_for_by_default():
    coordinator = make_coordinator(acking=["edge"])
    with pytest.raises(TimeoutError):
        coordinator.publish(ALL_EDGE, "edge", timeout=1.5)
    assert len(coordinator.client.published) == 1  # never put in force


def test_placement_must_stay_within_the_deployed_sites():
    coordinator = make_coordinator(acking=["edge"])
    with pytest.raises(ValueError):
        coordinator.publish({**ALL_EDGE, "grep": "cloud"}, "edge", timeout=1, sites=["edge"])
    assert coordinator.client.published == []
