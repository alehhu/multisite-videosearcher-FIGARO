import json
import os
import time

import paho.mqtt.client as mqtt


MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto-service")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "pipeline/done")
MQTT_CONNECT_RETRY_SECONDS = float(os.getenv("MQTT_CONNECT_RETRY_SECONDS", "5"))

LOG_DIR = os.getenv("LOG_DIR", "/var/log/pipeline-timings")
LOG_FILE = os.getenv("LOG_FILE", "timing.jsonl")

STAGE_MAP = {
    "groupA": "ffmpeg0",
    "groupB": "librosa",
    "groupC": "ffmpeg1",
    "groupD": "ffmpeg2",
    "groupE": "deepspeech",
    "groupF": "grep",
}


def compute_stage_records(timestamps):
    records = []
    for group, stage in STAGE_MAP.items():
        start_key = f"{group}_start"
        end_key = f"{group}_end"
        start = timestamps.get(start_key)
        end = timestamps.get(end_key)
        if start is None or end is None:
            continue
        try:
            duration = float(end) - float(start)
        except (TypeError, ValueError):
            continue
        records.append(
            {
                "group": group,
                "stage": stage,
                "start": start,
                "end": end,
                "duration": duration,
            }
        )
    return records


def append_record(record):
    os.makedirs(LOG_DIR, exist_ok=True)
    path = os.path.join(LOG_DIR, LOG_FILE)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, separators=(",", ":")) + "\n")


def on_connect(client, userdata, flags, rc, props=None):
    print(f"[TimingCollector] Connected to MQTT broker with result code: {rc}")
    client.subscribe(MQTT_TOPIC, qos=1)
    print(f"[TimingCollector] Subscribed to {MQTT_TOPIC}")


def on_message(client, userdata, msg, props=None):
    try:
        payload = json.loads(msg.payload.decode())
    except Exception as exc:
        print(f"[TimingCollector] Failed to decode payload: {exc}")
        return

    timestamps = payload.get("timestamps") or {}
    stage_records = compute_stage_records(timestamps)

    durations = {}
    for record in stage_records:
        durations[record["group"]] = record["duration"]
        durations[record["stage"]] = record["duration"]

    record = {
        "job_id": payload.get("dir") or payload.get("job_id"),
        "topic": msg.topic,
        "logged_at": time.time(),
        "timestamps": timestamps,
        "stages": stage_records,
        "durations": durations,
    }

    try:
        append_record(record)
    except OSError as exc:
        print(f"[TimingCollector] Failed to persist record: {exc}")
        return

    print(
        f"[TimingCollector] Logged job={record['job_id']} stages={len(stage_records)}"
    )


def build_client():
    if hasattr(mqtt, "CallbackAPIVersion"):
        client = mqtt.Client(
            protocol=mqtt.MQTTv5, callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        )
    else:
        client = mqtt.Client(protocol=mqtt.MQTTv5)
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.on_connect = on_connect
    client.on_message = on_message
    return client


def main():
    client = build_client()
    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT)
            client.loop_forever()
        except OSError as exc:
            print(
                f"[TimingCollector] MQTT connection error ({exc}). "
                f"Retrying in {MQTT_CONNECT_RETRY_SECONDS}s..."
            )
            time.sleep(MQTT_CONNECT_RETRY_SECONDS)


if __name__ == "__main__":
    main()
