import importlib.util
import json
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


def _install_fake_paho_if_missing():
    if "paho.mqtt.client" in sys.modules:
        return

    client_mod = types.ModuleType("paho.mqtt.client")
    client_mod.MQTTv5 = object()

    class _CallbackAPIVersion:
        VERSION2 = object()

    client_mod.CallbackAPIVersion = _CallbackAPIVersion
    client_mod.Client = lambda *args, **kwargs: None

    mqtt_mod = types.ModuleType("paho.mqtt")
    mqtt_mod.client = client_mod

    paho_mod = types.ModuleType("paho")
    paho_mod.mqtt = mqtt_mod

    sys.modules["paho"] = paho_mod
    sys.modules["paho.mqtt"] = mqtt_mod
    sys.modules["paho.mqtt.client"] = client_mod


def _load_collector_module():
    _install_fake_paho_if_missing()
    module_path = Path(__file__).with_name("collector.py")
    spec = importlib.util.spec_from_file_location("collector_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


collector = _load_collector_module()


def _make_msg(payload_bytes, topic="pipeline/done"):
    return SimpleNamespace(payload=payload_bytes, topic=topic)


def test_compute_stage_records_skips_missing_and_invalid_values():
    timestamps = {
        "groupA_start": "1.0",
        "groupA_end": "2.2",
        "groupB_start": "bad",
        "groupB_end": "3.0",
        "groupC_start": "5.0",
    }

    records = collector.compute_stage_records(timestamps)

    assert len(records) == 1
    assert records[0]["group"] == "groupA"
    assert records[0]["stage"] == "ffmpeg0"
    assert records[0]["duration"] == pytest.approx(1.2)


def test_append_record_writes_jsonl_line(tmp_path, monkeypatch):
    monkeypatch.setattr(collector, "LOG_DIR", str(tmp_path))
    monkeypatch.setattr(collector, "LOG_FILE", "timing.jsonl")

    record = {"job_id": "job-1", "durations": {"ffmpeg0": 0.4}}
    collector.append_record(record)

    output_file = tmp_path / "timing.jsonl"
    lines = output_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0]) == record


def test_on_message_invalid_payload_does_not_persist(monkeypatch, capsys):
    append_mock = Mock()
    monkeypatch.setattr(collector, "append_record", append_mock)

    msg = _make_msg(b"{not-valid-json")
    collector.on_message(None, None, msg)

    append_mock.assert_not_called()
    captured = capsys.readouterr()
    assert "Failed to decode payload" in captured.out


def test_on_message_persist_oserror_is_handled(monkeypatch, capsys):
    def raise_oserror(_record):
        raise OSError("disk full")

    monkeypatch.setattr(collector, "append_record", raise_oserror)

    payload = {
        "dir": "job-2",
        "timestamps": {"groupA_start": 10.0, "groupA_end": 10.5},
    }
    msg = _make_msg(json.dumps(payload).encode("utf-8"))
    collector.on_message(None, None, msg)

    captured = capsys.readouterr()
    assert "Failed to persist record" in captured.out


def test_on_message_builds_record_with_group_and_stage_durations(monkeypatch):
    persisted = []

    monkeypatch.setattr(collector, "append_record", lambda record: persisted.append(record))
    monkeypatch.setattr(collector.time, "time", lambda: 123.456)

    payload = {
        "dir": "job-3",
        "timestamps": {
            "groupA_start": 100.0,
            "groupA_end": 101.25,
            "groupB_start": 101.5,
            "groupB_end": 102.0,
        },
    }
    msg = _make_msg(json.dumps(payload).encode("utf-8"), topic="pipeline/custom")

    collector.on_message(None, None, msg)

    assert len(persisted) == 1
    record = persisted[0]
    assert record["job_id"] == "job-3"
    assert record["topic"] == "pipeline/custom"
    assert record["logged_at"] == 123.456
    assert record["durations"]["groupA"] == 1.25
    assert record["durations"]["ffmpeg0"] == 1.25
    assert record["durations"]["groupB"] == 0.5
    assert record["durations"]["librosa"] == 0.5


def test_on_connect_subscribes_to_configured_topic(monkeypatch):
    client = Mock()
    monkeypatch.setattr(collector, "MQTT_TOPIC", "pipeline/testing")

    collector.on_connect(client, None, None, 0)

    client.subscribe.assert_called_once_with("pipeline/testing", qos=1)


def test_build_client_sets_handlers_and_reconnect_delay(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.on_connect = None
            self.on_message = None
            self.reconnect_delay = None

        def reconnect_delay_set(self, min_delay, max_delay):
            self.reconnect_delay = (min_delay, max_delay)

    fake_client = FakeClient()

    def fake_client_ctor(*_args, **_kwargs):
        return fake_client

    monkeypatch.setattr(collector.mqtt, "Client", fake_client_ctor)

    client = collector.build_client()

    assert client is fake_client
    assert client.reconnect_delay == (1, 30)
    assert client.on_connect is collector.on_connect
    assert client.on_message is collector.on_message
