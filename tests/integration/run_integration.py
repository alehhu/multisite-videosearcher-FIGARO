#!/usr/bin/env python3
"""End-to-end test of the multi-site mechanics on one machine.

Two real Mosquitto brokers (edge, cloud) in Docker, linked by the bridge config from
deploy/mosquitto; the real gateway, timing collector and local controllers; workers are
local processes running fake_stage.py (same file chain as the real functions).

While jobs keep arriving, the placement changes several times (all-edge -> alternating ->
split -> all-cloud -> all-edge). The test then checks that:
  - every job completed exactly once, nothing went to the DLQ;
  - each job went to the site given by the placement version its sender was using;
  - the seed was used exactly when consecutive stages ran on different sites;
  - no job directory is left on either site;
  - every transition completed (drained, nothing lost in flight);
  - latency probes (scripts/link_probe.py) cross the bridge and come back.

    python tests/integration/run_integration.py [--bridge-from edge|cloud] [--jobs-per-s 2]
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import urllib.request

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "common"))
sys.path.insert(0, os.path.join(ROOT, "controller"))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import multisite  # noqa: E402
from multisite import GROUPS, SITES, STAGES, Placement  # noqa: E402
from local_controller import LocalController  # noqa: E402
from placement import Coordinator, load_placement_file  # noqa: E402
from process_backend import ProcessBackend  # noqa: E402

# Inside $HOME so that Docker (colima) can bind-mount it.
RUN_DIR = os.path.join(ROOT, "tests", "integration", ".run")
PORTS = {"edge": 18831, "cloud": 18832}
GATEWAY_PORT = 18080
NETWORK = "multisite-test"
MOSQUITTO_IMAGE = os.getenv("MOSQUITTO_IMAGE", "eclipse-mosquitto:2.0")

CONTROLLER_CONFIG = {
    "sample_interval_s": 0.5,
    "observation_interval_s": 5,
    "drain_quiet_s": 2,
    "drain_timeout_s": 120,
    "ready_timeout_s": 60,
    "stages": {
        "ffmpeg0": {"replicas": 1, "res_time_threshold": 1},
        "librosa": {"replicas": 1, "res_time_threshold": 1},
        "ffmpeg1": {"replicas": 1, "res_time_threshold": 1},
        "ffmpeg2": {"res_time_threshold": 1, "hpa": {"min": 1, "max": 3, "cpu_target": 60}},
        "deepspeech": {"res_time_threshold": 1, "hpa": {"min": 1, "max": 3, "cpu_target": 60}},
        "grep": {"replicas": 1, "res_time_threshold": 1},
    },
}

SEQUENCE = ["all-edge", "alternating", "split-ffmpeg2", "all-cloud", "all-edge"]


def sh(*cmd, check=True):
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


def start_brokers(bridge_from):
    sh("docker", "rm", "-f", "ms-edge", "ms-cloud", check=False)
    sh("docker", "network", "rm", NETWORK, check=False)
    sh("docker", "network", "create", NETWORK)
    for site in SITES:
        conf = os.path.join(RUN_DIR, f"broker-{site}")
        os.makedirs(os.path.join(conf, "bridge"))
        shutil.copy(os.path.join(ROOT, "deploy", "k8s", "base", "mosquitto.conf"), conf)
        if site == bridge_from:
            other = "cloud" if site == "edge" else "edge"
            with open(os.path.join(ROOT, "deploy", "mosquitto", f"bridge-from-{site}.conf")) as f:
                text = f.read().replace(f"{other.upper()}_BROKER_ADDRESS", f"ms-{other}")
            with open(os.path.join(conf, "bridge", "bridge.conf"), "w") as f:
                f.write(text)
    # The broker that opens the bridge starts last, so the other one is already listening.
    for site in sorted(SITES, key=lambda s: s == bridge_from):
        sh("docker", "run", "-d", "--name", f"ms-{site}", "--network", NETWORK,
           "-p", f"{PORTS[site]}:1883",
           "-v", f"{os.path.join(RUN_DIR, f'broker-{site}')}:/mosquitto/config",
           MOSQUITTO_IMAGE)
    time.sleep(2)


def stop_brokers():
    sh("docker", "rm", "-f", "ms-edge", "ms-cloud", check=False)
    sh("docker", "network", "rm", NETWORK, check=False)


def make_seeds(ramdisk):
    for stage in STAGES[1:]:
        seed = os.path.join(ramdisk, "seed", stage)
        os.makedirs(seed)
        with open(os.path.join(seed, f"{multisite.previous_stage(stage)}.out"), "w") as f:
            f.write("seed")


def start_process(name, args, env):
    log = open(os.path.join(RUN_DIR, f"{name}.log"), "w")
    return subprocess.Popen([sys.executable] + args, env=dict(os.environ, PYTHONUNBUFFERED="1", **env),
                            stdout=log, stderr=subprocess.STDOUT)


class Results:
    """Collects done/dlq messages at the gateway site."""

    def __init__(self):
        self.done = {}
        self.duplicates = 0
        self.dlq = []
        self.lock = threading.Lock()
        self.client = multisite.make_client()
        self.client.on_connect = lambda c, *a: (c.subscribe(multisite.done_topic("edge"), qos=1),
                                                c.subscribe(multisite.dlq_topic("edge"), qos=1))
        self.client.on_message = self._on_message
        multisite.connect(self.client, "localhost", PORTS["edge"])
        self.client.loop_start()

    def _on_message(self, client, userdata, msg, props=None):
        payload = json.loads(msg.payload)
        with self.lock:
            if msg.topic.endswith("/dlq"):
                self.dlq.append(payload)
            elif payload["dir"] in self.done:
                self.duplicates += 1
            else:
                self.done[payload["dir"]] = payload


def send_jobs(rate, stop, sent):
    i = 0
    while not stop.is_set():
        job = f"job-{i:05d}"
        body = json.dumps({"params": {"dir": job}, "Async": True}).encode()
        req = urllib.request.Request(f"http://127.0.0.1:{GATEWAY_PORT}/workflow/invoke/videosearcher_workflow",
                                     data=body, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                if resp.status == 202:
                    sent.append(job)
        except Exception as exc:
            print(f"  send {job} failed: {exc}")
        i += 1
        stop.wait(1.0 / rate)


def wait_http(url, timeout):
    deadline = time.time() + timeout
    while True:
        try:
            with urllib.request.urlopen(url, timeout=2):
                return
        except Exception:
            if time.time() > deadline:
                raise
            time.sleep(0.5)


def check(results, sent, tables, ramdisks):
    errors = []
    missing = sorted(set(sent) - set(results.done))
    if missing:
        errors.append(f"{len(missing)} jobs never completed, e.g. {missing[:5]}")
    if results.duplicates:
        errors.append(f"{results.duplicates} duplicate completions")
    if results.dlq:
        errors.append(f"{len(results.dlq)} jobs in DLQ, e.g. {results.dlq[0].get('error', {}).get('message')}")

    seeds_used = 0
    for job, p in results.done.items():
        sites, versions, ts = p["sites"], p["placement_versions"], p["timestamps"]
        sender = "gateway"
        for stage in STAGES:
            expected = tables[versions[sender]].site_of(stage)
            if sites[stage] != expected:
                errors.append(f"{job}: {stage} ran on {sites[stage]}, v{versions[sender]} says {expected}")
            if stage != STAGES[0]:
                prev = multisite.previous_stage(stage)
                used_seed = f"{GROUPS[stage]}_seed_start" in ts
                seeds_used += used_seed
                if used_seed != (sites[prev] != sites[stage]):
                    errors.append(f"{job}: seed used={used_seed} for {stage} ({sites[prev]}->{sites[stage]})")
            sender = stage

    for site, ramdisk in ramdisks.items():
        leftovers = [d for d in os.listdir(ramdisk) if d.startswith("job-")]
        if leftovers:
            errors.append(f"{site}: {len(leftovers)} job directories left, e.g. {leftovers[:3]}")

    events = []
    for site in SITES:
        with open(os.path.join(RUN_DIR, f"logs-{site}", "transitions.jsonl")) as f:
            events += [json.loads(line) for line in f]
    drained = [e for e in events if e["event"] == "drained"]
    bad = [e for e in events if e["event"] in ("drain_timeout", "ready_timeout")
           or (e["event"] == "drained" and e["lost_in_flight"])]
    if bad:
        errors.append(f"problematic transitions: {bad}")

    print(f"\n{len(sent)} sent, {len(results.done)} completed, {results.duplicates} duplicates, "
          f"{len(results.dlq)} DLQ, {seeds_used} seed copies, {len(drained)} stages drained")
    return errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bridge-from", choices=SITES, default="edge")
    parser.add_argument("--jobs-per-s", type=float, default=2.0)
    parser.add_argument("--seconds-per-placement", type=float, default=12.0)
    parser.add_argument("--service-s", type=float, default=0.15)
    args = parser.parse_args()

    shutil.rmtree(RUN_DIR, ignore_errors=True)
    os.makedirs(RUN_DIR)
    ramdisks = {}
    for site in SITES:
        ramdisks[site] = os.path.join(RUN_DIR, f"ramdisk-{site}")
        make_seeds(ramdisks[site])

    print(f"starting brokers (bridge opened by {args.bridge_from})")
    start_brokers(args.bridge_from)
    procs, backends, controllers, threads = [], {}, {}, []
    stop_controllers = threading.Event()
    try:
        for site in SITES:
            backends[site] = ProcessBackend(site, PORTS[site], ramdisks[site], RUN_DIR,
                                            {s: args.service_s for s in STAGES})
            controllers[site] = LocalController(site, CONTROLLER_CONFIG, backends[site], "localhost",
                                                PORTS[site], log_dir=os.path.join(RUN_DIR, f"logs-{site}"))
            t = threading.Thread(target=controllers[site].run, args=(stop_controllers,), daemon=True)
            t.start()
            threads.append(t)

        procs.append(start_process("gateway", [os.path.join(ROOT, "gateway", "gateway.py")],
                                   {"SITE": "edge", "MQTT_BROKER": "localhost", "MQTT_PORT": str(PORTS["edge"]),
                                    "PORT": str(GATEWAY_PORT), "HOST": "127.0.0.1",
                                    "PYTHONPATH": os.path.join(ROOT, "common")}))
        procs.append(start_process("collector", [os.path.join(ROOT, "monitoring", "timing_collector", "collector.py")],
                                   {"MQTT_BROKER": "localhost", "MQTT_PORT": str(PORTS["edge"]),
                                    "MQTT_TOPIC": multisite.done_topic("edge"),
                                    "LOG_DIR": os.path.join(RUN_DIR, "logs-edge")}))
        wait_http(f"http://127.0.0.1:{GATEWAY_PORT}/health", 60)
        results = Results()
        coordinator = Coordinator("localhost", PORTS["cloud"])

        tables = {}
        sent, stop_sending = [], threading.Event()
        sender = None
        for i, name in enumerate(SEQUENCE):
            stages, gateway = load_placement_file(os.path.join(ROOT, "placements", f"{name}.yaml"))
            t0 = time.time()
            placement = coordinator.publish(stages, gateway, timeout=120)
            tables[placement.version] = placement
            print(f"[{name}] v{placement.version} in force after {time.time() - t0:.1f}s")
            if sender is None:
                time.sleep(1)  # the gateway receives the first placement
                sender = threading.Thread(target=send_jobs, args=(args.jobs_per_s, stop_sending, sent), daemon=True)
                sender.start()
            if i < len(SEQUENCE) - 1:
                time.sleep(args.seconds_per_placement)

        stop_sending.set()
        sender.join()
        print(f"stopped sending after {len(sent)} jobs, waiting for completion...")
        deadline = time.time() + 120
        while time.time() < deadline and len(results.done) + len(results.dlq) < len(sent):
            time.sleep(0.5)
        # Let the last drains finish.
        coordinator.wait_for(lambda: all(not coordinator.status.get(s, {}).get("draining") for s in SITES),
                             60, "final drains")
        time.sleep(1)

        errors = check(results, sent, tables, ramdisks)
        for dst in ("cloud", "edge"):
            probe = subprocess.run(
                [sys.executable, os.path.join(ROOT, "scripts", "link_probe.py"),
                 "--broker", f"localhost:{PORTS['edge']}", "--from", "edge", "--to", dst,
                 "-n", "50", "--interval", "0.02"],
                capture_output=True, text=True)
            print(probe.stdout.strip())
            if probe.returncode != 0:
                errors.append(f"link probe edge->{dst} lost replies")
    finally:
        stop_controllers.set()
        for t in threads:
            t.join(timeout=10)
        for b in backends.values():
            b.shutdown()
        for p in procs:
            p.terminate()
            p.wait(timeout=10)
        stop_brokers()

    if errors:
        print("FAILED:")
        for e in errors[:30]:
            print("  -", e)
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
