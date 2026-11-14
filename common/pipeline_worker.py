"""MQTT wrapper shared by every pipeline stage.

Each worker is connected only to the broker of its own site. Jobs entering
`pipeline/<SITE>/<STAGE>` wait in the dispatcher of the site (dispatcher/dispatcher.py),
which hands each one to a free worker on the worker's own topic: a worker says it is
idle after starting and after every job, and that it leaves on SIGTERM. After the
handler runs, the worker looks up the current placement table (retained on
`control/placement/current`) and publishes the job to the site that hosts the next stage. Messages only carry the job id and timestamps: files stay on the site's
ramdisk, and a job arriving from the other site gets its input from the local seed copy.
A worker removes the job directory when the job leaves its site: the files on a ramdisk
count against the memory limit of the pod that wrote them, so they must not pile up.
What stays after a failure or a crash is removed by scripts/clean-ramdisk.sh.
Since the files go, the message records what each stage wrote: `outputs[<stage>]` maps
every file the stage created or changed in the job directory to its size in bytes.
"""

import json
import os
import queue
import shutil
import signal
import socket
import threading
import time
import traceback

import multisite
from multisite import GROUPS, STAGES, Placement



def files_in(directory):
    """{relative path: (size, mtime)} of the files under `directory` ({} if it does not exist)."""
    found = {}
    for root, _, names in os.walk(directory):
        for name in names:
            path = os.path.join(root, name)
            try:
                st = os.stat(path)
            except OSError:  # removed meanwhile
                continue
            found[os.path.relpath(path, directory)] = (st.st_size, st.st_mtime_ns)
    return found


def written_since(before, after):
    """{relative path: size} of the files created or changed between two `files_in` calls."""
    return {path: size for path, (size, mtime) in sorted(after.items()) if before.get(path) != (size, mtime)}


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
        self.ready_file = ready_file

        # Unique per process: a restarted container is a new worker for the dispatcher.
        self.worker_id = f"{pod_name or socket.gethostname()}-{os.getpid()}"
        self.job_topic = multisite.stage_topic(site, stage)
        self.inbox = multisite.work_topic(site, stage, self.worker_id)
        self.state_topic = multisite.worker_state_topic(site, stage)
        self.hello_topic = multisite.dispatcher_hello_topic(site)

        self.placement = None
        self.placement_known = threading.Event()
        self.lock = threading.RLock()  # re-entrant: the SIGTERM handler runs on the main thread
        self.inbox_subscribe_mid = None
        self.stopping = False
        self.busy = False       # processing a job
        self.left = False       # told the dispatcher it leaves: takes no more jobs
        self.last_job = None    # dir of the last job processed
        self.jobs = queue.Queue()

        # manual_ack: the job is acknowledged only after it has been processed and
        # forwarded.
        self.client = multisite.make_client(manual_ack=True)
        # If the worker dies, the broker tells the dispatcher, which gives its job to another.
        self.client.will_set(self.state_topic, self._state_payload("lost"), qos=1)
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
            ready_file=os.getenv("READY_FILE", "/tmp/worker-ready"),
        )

    # MQTT callbacks (network thread)

    def _on_connect(self, client, userdata, flags, rc, props=None):
        # Also after a reconnection. A job can arrive before the placement: it waits for
        # it in _process.
        client.subscribe(multisite.PLACEMENT_TOPIC, qos=1)
        client.subscribe(self.hello_topic, qos=1)
        _, self.inbox_subscribe_mid = client.subscribe(self.inbox, qos=1)
        print(f"[{self.stage}@{self.site}] connected as {self.worker_id}, jobs of {self.job_topic}", flush=True)

    def _on_subscribe(self, client, userdata, mid, reason_codes, props=None):
        # Ready once the broker has confirmed the subscription to the worker's own topic:
        # only then can it say it is idle. The Kubernetes readiness probe checks this file.
        if mid != self.inbox_subscribe_mid:
            return
        if self.ready_file and not self.stopping:
            with open(self.ready_file, "w") as f:
                f.write(str(time.time()))
        self._announce_if_idle()

    def _state_payload(self, state):
        return json.dumps({"worker": self.worker_id, "state": state, "last_job": self.last_job})

    def _announce(self, state):
        return self.client.publish(self.state_topic, self._state_payload(state), qos=1)

    def _announce_if_idle(self):
        with self.lock:
            if not (self.busy or self.stopping or self.left):
                self._announce("idle")

    def _on_message(self, client, userdata, msg, props=None):
        if msg.topic == multisite.PLACEMENT_TOPIC:
            self._on_placement(msg)
        elif msg.topic == self.hello_topic:
            # The dispatcher (re)started and does not know who is idle.
            self.client.ack(msg.mid, msg.qos)
            self._announce_if_idle()
        elif self.left:
            # Sent before the dispatcher knew this worker leaves: it puts the job back
            # in the queue, so it must not run here.
            self.client.ack(msg.mid, msg.qos)
            print(f"[{self.stage}@{self.site}] left: not taking a job sent meanwhile", flush=True)
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

    # Graceful shutdown

    def _on_sigterm(self, signum, frame):
        """Stop saying idle, finish the job in hand (and one already delivered), then leave.

        Leaving tells the dispatcher the last job done: a job it sent after that one goes
        back to its queue.
        """
        print(f"[{self.stage}@{self.site}] SIGTERM: finishing and leaving", flush=True)
        with self.lock:
            self.stopping = True
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

        while True:
            try:
                msg = self.jobs.get(timeout=0.5)
            except queue.Empty:
                if self.stopping:
                    break
                continue
            with self.lock:
                self.busy = True
            job = self._process(msg)
            self.client.ack(msg.mid, msg.qos)
            with self.lock:
                self.busy = False
                self.last_job = job
            self._announce_if_idle()

        with self.lock:
            self.left = True
        self._announce("leaving").wait_for_publish(timeout=10)
        # A clean disconnection: the broker does not send the last will.
        self.client.disconnect()
        self.client.loop_stop()
        print(f"[{self.stage}@{self.site}] left, exiting", flush=True)

    def _process(self, msg):
        """Run the job and forward it. Returns its dir (None for a malformed message)."""
        try:
            params = json.loads(msg.payload.decode())
        except ValueError:
            params = None
        if not isinstance(params, dict) or not params.get("dir"):
            print(f"[{self.stage}@{self.site}] dropping malformed message", flush=True)
            return None

        job = params["dir"]
        job_dir = os.path.join(self.ramdisk, job)
        ts = params.setdefault("timestamps", {})
        ts[f"{self.group}_received"] = time.time()
        params.setdefault("sites", {})[self.stage] = self.site
        params.setdefault("pods", {})[self.stage] = self.pod_name

        error = None
        try:
            if self._needs_seed(params, job_dir):
                # The previous stage ran on the other site: its output is not here.
                ts[f"{self.group}_seed_start"] = time.time()
                self._copy_seed(job_dir)
                ts[f"{self.group}_seed_end"] = time.time()
            if self.seed_capture and self.stage != STAGES[0]:
                self._capture_seed(job_dir)

            before = files_in(job_dir)
            ts[f"{self.group}_start"] = time.time()
            params = self.handler_fn(params, context={})
        except Exception as exc:
            error = {"stage": self.stage, "site": self.site, "message": str(exc),
                     "traceback": traceback.format_exc()}
            print(f"[{self.stage}@{self.site}] job {params.get('dir')} failed: {exc}", flush=True)
        ts[f"{self.group}_end"] = time.time()
        if error is None:
            # After the end timestamp: listing the files is not part of the service time.
            params.setdefault("outputs", {})[self.stage] = written_since(before, files_in(job_dir))

        # Normally already known: see _on_connect.
        self.placement_known.wait()
        with self.lock:
            placement = self.placement
        if error is not None:
            params["error"] = error
            destination, topic = placement.gateway, multisite.dlq_topic(placement.gateway)
        else:
            destination, topic = placement.destination(self.stage)
        params.setdefault("placement_versions", {})[self.stage] = placement.version

        # The job leaves this site: if it comes back, its input comes from the seed. Before
        # publishing, or this could remove the files of a job already back here. The
        # directory of a failed job stays, to look at it.
        if error is None and destination != self.site:
            shutil.rmtree(job_dir, ignore_errors=True)

        ts[f"{self.group}_published"] = time.time()
        info = self.client.publish(topic, json.dumps(params), qos=1)
        info.wait_for_publish(timeout=60)
        return job

    def _needs_seed(self, params, job_dir):
        """True when this stage's input is not on this site.

        Decided from where the previous stage ran, not from whether the job directory
        exists: directories are kept, so a job coming back to a site it already visited
        (edge -> cloud -> edge) finds its old directory, without the input it needs now.
        """
        previous = multisite.previous_stage(self.stage)
        if previous is None:
            return False
        previous_site = params.get("sites", {}).get(previous)
        if previous_site is None:
            # Message not produced by the pipeline (e.g. injected by hand).
            return not os.path.isdir(job_dir)
        return previous_site != self.site

    def _copy_seed(self, job_dir):
        src = os.path.join(self.seed_dir, self.stage)
        if not os.path.isdir(src):
            raise FileNotFoundError(f"no seed for {self.stage} at {src}")
        # The directory may exist from an earlier visit of this job to this site.
        shutil.copytree(src, job_dir, dirs_exist_ok=True)

    def _capture_seed(self, job_dir):
        """Save this stage's input as the seed used when a job arrives from the other site."""
        dst = os.path.join(self.seed_dir, self.stage)
        shutil.rmtree(dst, ignore_errors=True)
        shutil.copytree(job_dir, dst)
        print(f"[{self.stage}@{self.site}] seed captured in {dst}", flush=True)
