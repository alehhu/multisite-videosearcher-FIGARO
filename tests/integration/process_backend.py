"""Controller backend that runs each replica as a local process instead of a pod.

Mirrors what KubeBackend reports: a replica is *ready* once its worker has confirmed the
job subscription (ready file), *terminating* after SIGTERM until it has drained and exited.
"""

import os
import signal
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "common"))
from multisite import STAGES  # noqa: E402

FAKE_STAGE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fake_stage.py")


class Replica:
    def __init__(self, proc, ready_file):
        self.proc = proc
        self.ready_file = ready_file
        self.terminating = False

    def alive(self):
        return self.proc.poll() is None


class ProcessBackend:

    def __init__(self, site, broker_port, ramdisk, run_dir, service_s):
        self.site = site
        self.broker_port = broker_port
        self.ramdisk = ramdisk
        self.run_dir = run_dir
        self.service_s = service_s
        self.replicas = {s: [] for s in STAGES}
        self.hpas = set()
        self.counter = 0

    def _reap(self, stage):
        self.replicas[stage] = [r for r in self.replicas[stage] if r.alive()]

    def _spawn(self, stage):
        self.counter += 1
        name = f"{stage}-{self.site}-{self.counter}"
        ready_file = os.path.join(self.run_dir, f"ready-{name}")
        env = dict(os.environ, STAGE=stage, SITE=self.site, MQTT_BROKER="localhost",
                   MQTT_PORT=str(self.broker_port), POD_NAME=name, RAMDISK=self.ramdisk,
                   READY_FILE=ready_file, DRAIN_QUIET_SECONDS="1",
                   FAKE_SERVICE_S=str(self.service_s.get(stage, 0.2)), PYTHONUNBUFFERED="1")
        log = open(os.path.join(self.run_dir, f"worker-{name}.log"), "w")
        proc = subprocess.Popen([sys.executable, FAKE_STAGE], env=env, stdout=log, stderr=subprocess.STDOUT)
        self.replicas[stage].append(Replica(proc, ready_file))

    def scale(self, stage, replicas):
        self._reap(stage)
        active = [r for r in self.replicas[stage] if not r.terminating]
        for _ in range(replicas - len(active)):
            self._spawn(stage)
        for r in active[replicas:] if replicas < len(active) else []:
            r.proc.send_signal(signal.SIGTERM)
            r.terminating = True

    def ensure_hpa(self, stage, hpa):
        self.hpas.add(stage)

    def delete_hpa(self, stage):
        self.hpas.discard(stage)

    def desired(self, stage):
        self._reap(stage)
        return sum(1 for r in self.replicas[stage] if not r.terminating)

    def ready(self, stage):
        self._reap(stage)
        return sum(1 for r in self.replicas[stage]
                   if not r.terminating and os.path.exists(r.ready_file))

    def snapshot(self):
        result = {}
        for stage in STAGES:
            self._reap(stage)
            reps = self.replicas[stage]
            result[stage] = {
                "desired": sum(1 for r in reps if not r.terminating),
                "ready": sum(1 for r in reps if not r.terminating and os.path.exists(r.ready_file)),
                "alive": len(reps),
                "terminating": sum(1 for r in reps if r.terminating),
                "hpa": stage in self.hpas,
            }
        return result

    def cpu_per_stage(self):
        return {}

    def site_utilization(self):
        return {"nodes": None, "pipeline": None}

    def shutdown(self):
        for reps in self.replicas.values():
            for r in reps:
                if r.alive():
                    r.proc.send_signal(signal.SIGTERM)
        for reps in self.replicas.values():
            for r in reps:
                try:
                    r.proc.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    r.proc.kill()
