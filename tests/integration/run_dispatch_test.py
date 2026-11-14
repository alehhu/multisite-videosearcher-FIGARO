#!/usr/bin/env python3
"""Test of the dispatcher under a backlog, on one machine: one real Mosquitto in Docker,
the real dispatcher, workers running fake_stage.py.

  1. A long queue on one stage does not hold back another stage: 40 deepspeech jobs wait
     with no worker, and an ffmpeg2 job still goes through at once (the broker hands the
     dispatcher more than its default 20 messages in flight).
  2. Replicas started late take their share of the backlog: one deepspeech worker starts,
     three more a few seconds later; each of the late ones must serve several jobs.
  3. Nothing is lost or done twice when, with a backlog, one worker gets SIGTERM, one is
     killed, and the dispatcher itself is killed and restarted. The job of the killed
     worker is given to another one; like the real functions, the fake one has already
     removed its input, so it ends in the DLQ: failed and counted, not lost.

    python tests/integration/run_dispatch_test.py
"""

import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import Counter

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "common"))
import multisite  # noqa: E402
from multisite import STAGES, Placement  # noqa: E402

RUN_DIR = os.path.join(ROOT, "tests", "integration", ".run-dispatch")
RAMDISK = os.path.join(RUN_DIR, "ramdisk")
PORT = 18833
SITE = "edge"
SERVICE_S = 0.5
MOSQUITTO_IMAGE = os.getenv("MOSQUITTO_IMAGE", "eclipse-mosquitto:2.0")
FAKE_STAGE = os.path.join(ROOT, "tests", "integration", "fake_stage.py")


def sh(*cmd):
    return subprocess.run(cmd, capture_output=True, text=True)


def start_broker():
    sh("docker", "rm", "-f", "ms-dispatch")
    conf = os.path.join(RUN_DIR, "broker")
    os.makedirs(os.path.join(conf, "bridge"))
    shutil.copy(os.path.join(ROOT, "deploy", "k8s", "base", "mosquitto.conf"), conf)
    sh("docker", "run", "-d", "--name", "ms-dispatch", "-p", f"{PORT}:1883",
       "-v", f"{conf}:/mosquitto/config", MOSQUITTO_IMAGE)
    time.sleep(2)


def start_dispatcher(n):
    log = open(os.path.join(RUN_DIR, f"dispatcher-{n}.log"), "w")
    env = dict(os.environ, SITE=SITE, MQTT_BROKER="localhost", MQTT_PORT=str(PORT), REPORT_S="2",
               PYTHONPATH=os.path.join(ROOT, "common"), PYTHONUNBUFFERED="1")
    return subprocess.Popen([sys.executable, os.path.join(ROOT, "dispatcher", "dispatcher.py")],
                            env=env, stdout=log, stderr=subprocess.STDOUT)


class Workers:
    def __init__(self):
        self.procs = {}

    def start(self, stage, name):
        ready = os.path.join(RUN_DIR, f"ready-{name}")
        env = dict(os.environ, STAGE=stage, SITE=SITE, MQTT_BROKER="localhost", MQTT_PORT=str(PORT),
                   POD_NAME=name, RAMDISK=RAMDISK, READY_FILE=ready,
                   FAKE_SERVICE_S=str(SERVICE_S), PYTHONUNBUFFERED="1")
        log = open(os.path.join(RUN_DIR, f"worker-{name}.log"), "w")
        self.procs[name] = subprocess.Popen([sys.executable, FAKE_STAGE], env=env,
                                            stdout=log, stderr=subprocess.STDOUT)
        deadline = time.time() + 20
        while not os.path.exists(ready):
            if time.time() > deadline:
                raise RuntimeError(f"worker {name} not ready")
            time.sleep(0.1)

    def stop_all(self):
        for p in self.procs.values():
            if p.poll() is None:
                p.kill()


class Observer:
    """Every job that leaves deepspeech (to grep or the DLQ) and every one entering deepspeech."""

    def __init__(self):
        self.lock = threading.Lock()
        self.done = []          # job messages published by deepspeech
        self.failed = []        # job messages in the DLQ
        self.entered = set()    # jobs published to deepspeech
        self.client = multisite.make_client()
        self.client.on_connect = lambda c, *a: [c.subscribe(t, qos=1) for t in (
            multisite.stage_topic(SITE, "grep"), multisite.stage_topic(SITE, "deepspeech"),
            multisite.dlq_topic(SITE))]
        self.client.on_message = self._on_message
        multisite.connect(self.client, "localhost", PORT)
        self.client.loop_start()

    def _on_message(self, client, userdata, msg, props=None):
        params = json.loads(msg.payload)
        with self.lock:
            if msg.topic.endswith("/grep"):
                self.done.append(params)
            elif msg.topic.endswith("/dlq"):
                self.failed.append(params)
            else:
                self.entered.add(params["dir"])

    def wait_done(self, jobs, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self.lock:
                if set(jobs) <= {p["dir"] for p in self.done + self.failed}:
                    return True
            time.sleep(0.2)
        return False


def publish_jobs(client, stage, names):
    previous = multisite.previous_stage(stage)
    for name in names:
        os.makedirs(os.path.join(RAMDISK, name), exist_ok=True)
        with open(os.path.join(RAMDISK, name, f"{previous}.out"), "w") as f:
            f.write(name)
        client.publish(multisite.stage_topic(SITE, stage), json.dumps({"dir": name}), qos=1).wait_for_publish()


def main():
    shutil.rmtree(RUN_DIR, ignore_errors=True)
    os.makedirs(RAMDISK)
    start_broker()
    errors = []
    workers = Workers()
    dispatcher = start_dispatcher(1)
    try:
        client = multisite.make_client()
        multisite.connect(client, "localhost", PORT)
        client.loop_start()
        everything_here = Placement(1, SITE, {s: SITE for s in STAGES})
        client.publish(multisite.PLACEMENT_TOPIC, everything_here.to_json(), qos=1, retain=True).wait_for_publish()
        observer = Observer()
        time.sleep(1)

        # 1. A backlog on deepspeech, no worker for it; an ffmpeg2 job must not wait.
        backlog = [f"job-a{i:02d}" for i in range(40)]
        publish_jobs(client, "deepspeech", backlog)
        time.sleep(1)
        workers.start("ffmpeg2", "ffmpeg2-1")
        t0 = time.time()
        publish_jobs(client, "ffmpeg2", ["job-hol"])
        while "job-hol" not in observer.entered and time.time() - t0 < 10:
            time.sleep(0.05)
        elapsed = time.time() - t0
        print(f"1. ffmpeg2 job through in {elapsed:.2f} s with 40 deepspeech jobs waiting")
        if elapsed > 3:
            errors.append(f"an ffmpeg2 job waited {elapsed:.1f} s behind the deepspeech backlog")
        backlog.append("job-hol")

        # 2. One worker, then three more: the late ones must take part of the backlog.
        workers.start("deepspeech", "ds-early")
        time.sleep(3)
        for i in range(3):
            workers.start("deepspeech", f"ds-late{i}")
        if not observer.wait_done(backlog, 60):
            errors.append("the backlog was not served in 60 s")
        with observer.lock:
            per_pod = Counter(p["pods"]["deepspeech"] for p in observer.done if p["dir"] in backlog)
        print(f"2. {len(backlog)} jobs by replica: {dict(sorted(per_pod.items()))}")
        late = [n for pod, n in per_pod.items() if pod.startswith("ds-late")]
        if len(late) != 3 or min(late) < 5:
            errors.append(f"late replicas did not take their share of the backlog: {dict(per_pod)}")

        # 3. Failures with a backlog: SIGTERM, SIGKILL of a worker, the dispatcher killed.
        batch = [f"job-b{i:02d}" for i in range(30)]
        publish_jobs(client, "deepspeech", batch)
        time.sleep(1.2)
        workers.procs["ds-late0"].send_signal(signal.SIGTERM)
        workers.procs["ds-late1"].kill()
        time.sleep(1)
        dispatcher.kill()
        dispatcher.wait()
        time.sleep(2)
        dispatcher = start_dispatcher(2)
        if not observer.wait_done(batch, 60):
            errors.append("jobs lost after the failures")
        with observer.lock:
            ended = [p for p in observer.done + observer.failed if p["dir"] in batch]
            failed = [p for p in observer.failed if p["dir"] in batch]
        counts = Counter(p["dir"] for p in ended)
        requeued = {p["dir"] for p in ended if p.get("requeued", {}).get("deepspeech")}
        missing = sorted(set(batch) - set(counts))
        twice = sorted(j for j, n in counts.items() if n > 1)
        terminated = workers.procs["ds-late0"].wait(timeout=10)
        print(f"3. {len(counts)}/{len(batch)} ended ({len(failed)} in the DLQ), {len(twice)} twice, "
              f"requeued: {sorted(requeued)}, SIGTERM worker exited with {terminated}")
        if any(p["dir"] not in requeued for p in failed):
            errors.append(f"jobs failed that were not requeued: {[p['dir'] for p in failed]}")
        if missing:
            errors.append(f"{len(missing)} jobs lost: {missing[:5]}")
        if twice:
            errors.append(f"{len(twice)} jobs done twice: {twice[:5]}")
        if not requeued:
            errors.append("the job of the killed worker was not requeued")
        if terminated != 0:
            errors.append(f"the worker given SIGTERM exited with {terminated}")
    finally:
        workers.stop_all()
        dispatcher.kill()
        sh("docker", "rm", "-f", "ms-dispatch")

    if errors:
        print("FAILED:")
        for e in errors:
            print("  -", e)
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
