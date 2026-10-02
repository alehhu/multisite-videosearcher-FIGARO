"""MQTT wrapper shared by every pipeline stage.

Each worker is connected only to the broker of its own site and consumes jobs from
`pipeline/<SITE>/<STAGE>` through a shared subscription, so a job goes to exactly one
replica. After the handler runs, the worker looks up the current placement table
(retained on `control/placement/current`) and publishes the job to the site that hosts
the next stage. Messages only carry the job id and timestamps: files stay on the site's
ramdisk, and a job arriving from the other site gets its input from the local seed copy.
"""

import json
import os
import queue
import shutil
import signal
import threading
import time
import traceback

import multisite
from multisite import GROUPS, STAGES, Placement


class PipelineWorker:

    def __init__(
            self,
            stage,
            site,
            handler_fn,
            broker_host=None,
            broker_port=None,
            pod_name="",
            ramdisk="/mnt/ramdisk",
            seed_dir=None,
            seed_capture=False,
            drain_quiet_seconds=5.0,
            ready_file="/tmp/worker-ready",
            ):
        if stage not in STAGES:
            raise ValueError(f"unknown stage {stage!r}")
        self.stage = stage
        self.site = site
        self.group = GROUPS[stage]
        self.handler_fn = handler_fn
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.pod_name = pod_name
        self.ramdisk = ramdisk
        self.seed_dir = seed_dir or os.path.join(ramdisk, "seed")
        self.seed_capture = seed_capture
        self.drain_quiet_seconds = drain_quiet_seconds
        self.ready_file = ready_file

        self.job_topic = multisite.stage_topic(site, stage)
        # Shared subscription: replicas of the same stage on the same site form one group,
        # and the broker hands each job to only one of them.
        self.shared_topic = f"$share/{self.group}-{site}/{self.job_topic}"

        self.placement = None
        self.placement_known = threading.Event()
        self.lock = threading.RLock()  # re-entrant: the SIGTERM handler runs on the main thread
        self.subscribed = False
        self.job_subscribe_mid = None
        self.stopping = False
        self.jobs = queue.Queue()

        # manual_ack: the job is acknowledged only after it has been processed and
        # forwarded. Together with receive_maximum=1 the broker sends the next job only
        # when this one is done, so pending jobs wait in the broker.
        self.client = multisite.make_client(manual_ack=True)
        self.client.on_connect = self._on_connect
        self.client.on_subscribe = self._on_subscribe
        self.client.on_message = self._on_message

    @classmethod
    def from_env(cls, handler_fn):
        return cls(
            stage=os.environ["STAGE"],
            site=os.environ["SITE"],
            handler_fn=handler_fn,
            broker_host=os.getenv("MQTT_BROKER", "mosquitto-service"),
            broker_port=int(os.getenv("MQTT_PORT", "1883")),
            pod_name=os.getenv("POD_NAME", ""),
            ramdisk=os.getenv("RAMDISK", "/mnt/ramdisk"),
            seed_dir=os.getenv("SEED_DIR") or None,
            seed_capture=os.getenv("SEED_CAPTURE", "0") == "1",
            drain_quiet_seconds=float(os.getenv("DRAIN_QUIET_SECONDS", "5")),
            ready_file=os.getenv("READY_FILE", "/tmp/worker-ready"),
        )

    # MQTT callbacks (network thread)

    def _on_connect(self, client, userdata, flags, rc, props=None):
        client.subscribe(multisite.PLACEMENT_TOPIC, qos=1)
        with self.lock:
            # Subscribe to jobs right away (also after a reconnection), unless draining.
            # Jobs can only be routed here by a placement that this worker will also
            # receive, so a job arriving first just waits for the placement in _process.
            self.subscribed = False
            if not self.stopping:
                self._subscribe_jobs()
        print(f"[{self.stage}@{self.site}] connected, waiting for jobs on {self.job_topic}", flush=True)

    def _on_subscribe(self, client, userdata, mid, reason_codes, props=None):
        # The replica is ready only once the broker has confirmed the job subscription:
        # before that, jobs published to this site would be dropped (no subscriber).
        # The Kubernetes readiness probe checks this file.
        if mid == self.job_subscribe_mid and self.ready_file:
            with open(self.ready_file, "w") as f:
                f.write(str(time.time()))

    def _on_message(self, client, userdata, msg, props=None):
        if msg.topic == multisite.PLACEMENT_TOPIC:
            self._on_placement(msg)
        else:
            self.jobs.put(msg)

    def _on_placement(self, msg):
        # The placement message is not a job: acknowledge it right away.
        self.client.ack(msg.mid, msg.qos)
        if not msg.payload:
            return
        try:
            placement = Placement.from_json(msg.payload)
        except (ValueError, KeyError) as exc:
            print(f"[{self.stage}@{self.site}] ignoring invalid placement: {exc}", flush=True)
            return
        with self.lock:
            if self.placement is not None and placement.version <= self.placement.version:
                return
            self.placement = placement
            self.placement_known.set()
            print(f"[{self.stage}@{self.site}] placement v{placement.version}: {placement.stages}", flush=True)

    def _subscribe_jobs(self):
        _, self.job_subscribe_mid = self.client.subscribe(self.shared_topic, qos=1)
        self.subscribed = True

    # Graceful shutdown

    def _on_sigterm(self, signum, frame):
        """Stop taking new jobs, finish the ones already assigned to this replica, then exit.

        Mosquitto assigns shared-subscription messages to a client when they are published,
        and keeps delivering the ones already assigned after an unsubscribe: the worker keeps
        processing until no job has arrived for `drain_quiet_seconds`.
        """
        print(f"[{self.stage}@{self.site}] SIGTERM: draining", flush=True)
        with self.lock:
            self.stopping = True
            if self.subscribed:
                self.client.unsubscribe(self.shared_topic)
                self.subscribed = False
        if self.ready_file and os.path.exists(self.ready_file):
            os.remove(self.ready_file)

    # Job processing (main thread)

    def run(self):
        if self.ready_file and os.path.exists(self.ready_file):
            os.remove(self.ready_file)
        signal.signal(signal.SIGTERM, self._on_sigterm)
        signal.signal(signal.SIGINT, self._on_sigterm)
        multisite.connect(self.client, self.broker_host, self.broker_port, receive_maximum=1)
        self.client.loop_start()

        last_activity = time.time()
        while True:
            try:
                msg = self.jobs.get(timeout=0.5)
            except queue.Empty:
                if self.stopping and time.time() - last_activity >= self.drain_quiet_seconds:
                    break
                continue
            self._process(msg)
            self.client.ack(msg.mid, msg.qos)
            last_activity = time.time()

        self.client.disconnect()
        self.client.loop_stop()
        print(f"[{self.stage}@{self.site}] drained, exiting", flush=True)

    def _process(self, msg):
        try:
            params = json.loads(msg.payload.decode())
        except ValueError:
            params = None
        if not isinstance(params, dict) or not params.get("dir"):
            print(f"[{self.stage}@{self.site}] dropping malformed message", flush=True)
            return

        job_dir = os.path.join(self.ramdisk, params["dir"])
        ts = params.setdefault("timestamps", {})
        ts[f"{self.group}_received"] = time.time()
        params.setdefault("sites", {})[self.stage] = self.site
        params.setdefault("pods", {})[self.stage] = self.pod_name

        error = None
        try:
            if self.stage != STAGES[0] and not os.path.isdir(job_dir):
                # The previous stage ran on the other site: its output is not here.
                ts[f"{self.group}_seed_start"] = time.time()
                self._copy_seed(job_dir)
                ts[f"{self.group}_seed_end"] = time.time()
            if self.seed_capture and self.stage != STAGES[0]:
                self._capture_seed(job_dir)

            ts[f"{self.group}_start"] = time.time()
            params = self.handler_fn(params, context={})
        except Exception as exc:
            error = {"stage": self.stage, "site": self.site, "message": str(exc),
                     "traceback": traceback.format_exc()}
            print(f"[{self.stage}@{self.site}] job {params.get('dir')} failed: {exc}", flush=True)
        ts[f"{self.group}_end"] = time.time()

        # Normally already known: see _on_connect.
        self.placement_known.wait()
        with self.lock:
            placement = self.placement
        if error is not None:
            params["error"] = error
            dest_site, topic = placement.gateway, multisite.dlq_topic(placement.gateway)
        else:
            dest_site, topic = placement.destination(self.stage)
        params.setdefault("placement_versions", {})[self.stage] = placement.version

        ts[f"{self.group}_published"] = time.time()
        info = self.client.publish(topic, json.dumps(params), qos=1)
        info.wait_for_publish(timeout=60)

        # The job leaves this site (or is dead): its working directory is no longer needed.
        if (error is not None or dest_site != self.site) and os.path.isdir(job_dir):
            shutil.rmtree(job_dir, ignore_errors=True)

    def _copy_seed(self, job_dir):
        src = os.path.join(self.seed_dir, self.stage)
        if not os.path.isdir(src):
            raise FileNotFoundError(f"no seed for {self.stage} at {src}")
        shutil.copytree(src, job_dir)

    def _capture_seed(self, job_dir):
        """Save this stage's input as the seed used when a job arrives from the other site."""
        dst = os.path.join(self.seed_dir, self.stage)
        shutil.rmtree(dst, ignore_errors=True)
        shutil.copytree(job_dir, dst)
        print(f"[{self.stage}@{self.site}] seed captured in {dst}", flush=True)
