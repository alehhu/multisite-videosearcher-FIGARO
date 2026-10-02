import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "common"))
import multisite  # noqa: E402
from multisite import Placement  # noqa: E402

ALTERNATING = {"ffmpeg0": "edge", "librosa": "cloud", "ffmpeg1": "edge",
               "ffmpeg2": "cloud", "deepspeech": "edge", "grep": "cloud"}


def test_destination_follows_next_stage_site():
    p = Placement(3, "edge", ALTERNATING)
    assert p.entry() == ("edge", "pipeline/edge/ffmpeg0")
    assert p.destination("ffmpeg0") == ("cloud", "pipeline/cloud/librosa")
    assert p.destination("deepspeech") == ("cloud", "pipeline/cloud/grep")
    # The last stage reports to the gateway's site.
    assert p.destination("grep") == ("edge", "pipeline/edge/done")


def test_round_trip_json():
    p = Placement(7, "edge", ALTERNATING)
    q = Placement.from_json(p.to_json().encode())
    assert q.to_dict() == p.to_dict()


@pytest.mark.parametrize("stages, gateway", [
    ({**ALTERNATING, "grep": "mars"}, "edge"),
    ({k: v for k, v in ALTERNATING.items() if k != "grep"}, "edge"),
    (ALTERNATING, "nowhere"),
])
def test_invalid_placements_are_rejected(stages, gateway):
    with pytest.raises(ValueError):
        Placement(1, gateway, stages)


def test_parse_topic():
    assert multisite.parse_topic("pipeline/cloud/librosa") == ("cloud", "librosa")
    assert multisite.parse_topic("pipeline/edge/done") == ("edge", "done")
    assert multisite.parse_topic("control/placement/current") is None
