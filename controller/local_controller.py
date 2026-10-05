"""Local controller: one per site, talks only to its own cluster and its own broker.

Responsibilities:
  1. Placement transitions, make-before-break:
       - on a *pending* placement, start the stages that move onto this site and, once
         they are ready, ack it on control/status/<site>;
       - on the *current* placement, drain the stages that left this site (wait until every
         job that entered them here has come out) and only then scale them to zero.
  2. Scaling: a stage with an `agent` block is scaled by an external agent, asked at
     every observation window (POST <url>/action, same interface as the DQN agent server
     of ServerlessAutoscalingWithRL); one with an `hpa` block gets an HPA while it runs
     here; the others keep a fixed number of replicas.
  3. Measurements, written under LOG_DIR:
       pods.csv          desired / ready / alive / terminating replicas per stage
       transitions.jsonl every step of every placement change
       observations.jsonl agent-ready state (DQN per stage, master per site), also
                          published on observations/<site>, with the agents' decisions.
"""

import csv
import json
import os
import sys
import threading
import time
import urllib.request

import yaml

import multisite
from multisite import GROUPS, STAGES, Placement


def log(*args):
    print(time.strftime("[%Y-%m-%d %H:%M:%S]"), *args, flush=True)


class StageWindow:
    """Measurements of one stage on this site over one observation window."""

    def __init__(self):
        self.arrivals = 0
        self.service_times = []
        self.response_times = []
        self.ready_samples = []
        self.alive_samples = []
        self.cpu_samples = []


class LocalController:

    def __init__(self, site, config, backend, mqtt_host=None, mqtt_port=None, log_dir="logs"):
        self.site = site
        self.config = config
        self.backend = backend
        self.stage_cfg = config["stages"]
        self.sample_interval = config.get("sample_interval_s", 5)
        self.observation_interval = config.get("observation_interval_s", 120)
        self.drain_quiet = config.get("drain_quiet_s", 10)
        self.drain_timeout = config.get("drain_timeout_s", 1800)
        self.ready_timeout = config.get("ready_timeout_s", 600)
        self.agent_timeout = config.get("agent_timeout_s", 3)
        for stage, cfg in self.stage_cfg.items():
            if cfg.get("agent") and cfg.get("hpa"):
                raise ValueError(f"stage {stage}: configure either `agent` or `hpa`, not both")

        self.lock = threading.RLock()
        self.current = None          # Placement in force
        self.pending = None          # Placement announced, not yet in force
        self.acked_version = 0
        self.waiting_ready = {}      # stage -> {"since": t, "target": replicas to see ready}
        self.replica_counts = {}     # stage -> replicas of the stages settled here (in the status)
        self.draining = {}           # stage -> {"since": t, "version": v}

        # Jobs that entered a stage on this site and have not come out yet: stage -> {job: t}
        self.in_flight = {s: {} for s in STAGES}
        self.last_arrival = {s: 0.0 for s in STAGES}
        self.window = {s: StageWindow() for s in STAGES}
        self.window_start = time.time()
        self.lambda_arrivals = 0     # new jobs published by the gateway (gateway site only)

        os.makedirs(log_dir, exist_ok=True)
        self.transitions_path = os.path.join(log_dir, "transitions.jsonl")
        self.observations_path = os.path.join(log_dir, "observations.jsonl")
        pods_path = os.path.join(log_dir, "pods.csv")
        new_file = not os.path.exists(pods_path)
        self.pods_file = open(pods_path, "a", newline="")
        self.pods_csv = csv.writer(self.pods_file)
        if new_file:
            self.pods_csv.writerow(["timestamp", "site", "stage", "placed_here", "draining",
                                    "desired", "ready", "alive", "terminating", "hpa"])

        self.client = multisite.make_client()
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port

    # MQTT

    def _on_connect(self, client, userdata, flags, rc, props=None):
        client.subscribe(multisite.PLACEMENT_TOPIC, qos=1)
        client.subscribe(multisite.PLACEMENT_PENDING_TOPIC, qos=1)
        # Plain (non-shared) subscription: the controller observes every job message on
        # its broker without taking it away from the workers.
        client.subscribe("pipeline/#", qos=1)
        client.subscribe(multisite.probe_ping_topic(self.site), qos=1)
        log(f"[controller@{self.site}] connected")
        self._publish_status()

    def _on_message(self, client, userdata, msg, props=None):
        try:
            if msg.topic == multisite.PLACEMENT_TOPIC:
                if msg.payload:
                    self._on_current(Placement.from_json(msg.payload))
            elif msg.topic == multisite.PLACEMENT_PENDING_TOPIC:
                if msg.payload:
                    self._on_pending(Placement.from_json(msg.payload))
            elif msg.topic == multisite.probe_ping_topic(self.site):
                self._on_probe(msg)
            else:
                self._on_job_message(msg)
        except Exception as exc:  # never let a bad message kill the network thread
            log(f"[controller@{self.site}] error handling {msg.topic}: {exc!r}")

    def _on_probe(self, msg):
        """Echo a latency probe back to the topic it asks for (scripts/link_probe.py)."""
        probe = json.loads(msg.payload)
        probe["echoed_by"] = self.site
        probe["echoed_at"] = time.time()
        self.client.publish(probe["reply_to"], json.dumps(probe), qos=1)

    def _publish_status(self):
        with self.lock:
            status = {
                "site": self.site,
                "acked_version": self.acked_version,
                "current_version": self.current.version if self.current else 0,
                "draining": sorted(self.draining),
                "starting": sorted(self.waiting_ready),
                # Read by the placement coordinator: a stage that moves keeps its replicas.
                "replicas": dict(self.replica_counts),
                "t": time.time(),
            }
        self.client.publish(multisite.status_topic(self.site), json.dumps(status), qos=1, retain=True)

    # Job tracking

    def _on_job_message(self, msg):
        parsed = multisite.parse_topic(msg.topic)
        if parsed is None:
            return
        dest_site, target = parsed
        payload = json.loads(msg.payload.decode())
        job = payload.get("dir")
        if not job:
            return
        sites = payload.get("sites", {})
        now = time.time()

        with self.lock:
            # Arrival: a job addressed to a stage of this site.
            if dest_site == self.site and target in STAGES:
                self.in_flight[target][job] = now
                self.last_arrival[target] = now
                self.window[target].arrivals += 1
            # New job from the gateway of this site (wherever the first stage runs).
            if target == STAGES[0] and sites.get("gateway") == self.site:
                self.lambda_arrivals += 1

            # Departure: the message is the output of the stage before `target`.
            if target in STAGES:
                finished = multisite.previous_stage(target)
            elif target == "done":
                finished = STAGES[-1]
            elif target == "dlq":
                finished = (payload.get("error") or {}).get("stage")
            else:
                finished = None
            if finished and sites.get(finished) == self.site:
                self.in_flight[finished].pop(job, None)
                if target != "dlq":
                    self._record_completion(finished, payload.get("timestamps", {}))

    def _record_completion(self, stage, ts):
        group = GROUPS[stage]
        start, end = ts.get(f"{group}_start"), ts.get(f"{group}_end")
        if start is None or end is None:
            return
        prev = multisite.previous_stage(stage)
        prev_published = ts.get(f"{GROUPS[prev]}_published") if prev else ts.get("gateway_published")
        w = self.window[stage]
        w.service_times.append(end - start)
        if prev_published is not None:
            # Queueing + (if the previous stage is remote) WAN hop and seed copy + service.
            w.response_times.append(end - prev_published)

    # Placement transitions

    def _on_pending(self, placement):
        with self.lock:
            if placement.version <= self.acked_version:
                return
            if self.current is not None and placement.version <= self.current.version:
                return
            self.pending = placement
            incoming = [s for s in placement.stages_on(self.site)
                        if self.current is None or self.current.site_of(s) != self.site
                        or s in self.draining]
            self.waiting_ready = {}
            for stage in incoming:
                if stage in self.draining:
                    # Moving back here before the drain finished: keep it running.
                    del self.draining[stage]
                    self._event(placement.version, stage, "drain_cancelled")
                target = self._activate(stage, placement.replicas.get(stage))
                self.waiting_ready[stage] = {"since": time.time(), "target": target}
                self._event(placement.version, stage, "start_requested", replicas=target)
            log(f"[controller@{self.site}] pending v{placement.version}, starting {incoming}")
        self._check_ready()

    def _on_current(self, placement):
        with self.lock:
            if self.current is not None and placement.version <= self.current.version:
                return
            previous = self.current
            self.current = placement
            self.pending = None
            self.waiting_ready = {}
            self._event(placement.version, None, "placement_in_force")
            for stage in STAGES:
                here_now = placement.site_of(stage) == self.site
                if here_now:
                    # Normally started during the pending phase; this also covers a
                    # controller that (re)started after the pending message.
                    self.draining.pop(stage, None)
                    self._activate(stage)
                elif previous is not None and previous.site_of(stage) == self.site:
                    self.draining[stage] = {"since": time.time(), "version": placement.version}
                    self._event(placement.version, stage, "drain_started",
                                in_flight=len(self.in_flight[stage]))
                elif previous is None and self.backend.desired(stage) > 0:
                    # Controller started with replicas left over from an earlier run.
                    self.draining[stage] = {"since": time.time(), "version": placement.version}
                    self._event(placement.version, stage, "drain_started", reason="leftover")
            log(f"[controller@{self.site}] placement v{placement.version} in force, "
                f"here={placement.stages_on(self.site)} draining={sorted(self.draining)}")
        self._publish_status()

    def _activate(self, stage, initial=None):
        """Make sure the stage runs here. `initial` is the number of replicas it had on the
        site it is moving from; without it the stage starts with its configured `replicas`.
        Returns how many replicas must be ready before the stage counts as started."""
        cfg = self.stage_cfg[stage]
        hpa, agent = cfg.get("hpa"), cfg.get("agent")
        running = self.backend.desired(stage)
        if hpa:
            target = max(hpa["min"], min(initial or hpa["min"], hpa["max"]))
            if running < target:
                self.backend.scale(stage, target)
            self.backend.ensure_hpa(stage, hpa)
            return target
        target = initial or cfg.get("replicas", 1)
        if agent:
            high = agent.get("max", self.config.get("normalization", {}).get("max_n_instances", 10))
            target = max(agent.get("min", 1), min(target, high))
        if running > 0:
            return 1  # already running here (e.g. a cancelled drain): keep what it has
        self.backend.scale(stage, target)
        return target

    def _deactivate(self, stage):
        self.backend.delete_hpa(stage)
        self.backend.scale(stage, 0)

    def _check_ready(self):
        acked = False
        with self.lock:
            if self.pending is None:
                return
            now = time.time()
            for stage, wait in list(self.waiting_ready.items()):
                since = wait["since"]
                if self.backend.ready(stage) >= wait["target"]:
                    del self.waiting_ready[stage]
                    self._event(self.pending.version, stage, "ready", after_s=round(now - since, 3))
                elif now - since > self.ready_timeout:
                    self._event(self.pending.version, stage, "ready_timeout")
            if not self.waiting_ready and self.acked_version < self.pending.version:
                self.acked_version = self.pending.version
                self._event(self.pending.version, None, "acked")
                acked = True
        if acked:
            self._publish_status()

    def _check_drains(self):
        finished = []
        with self.lock:
            now = time.time()
            for stage, info in list(self.draining.items()):
                pending_jobs = len(self.in_flight[stage])
                quiet = now - max(self.last_arrival[stage], info["since"]) >= self.drain_quiet
                timed_out = now - info["since"] > self.drain_timeout
                if (pending_jobs == 0 and quiet) or timed_out:
                    self._deactivate(stage)
                    del self.draining[stage]
                    self._event(info["version"], stage, "drain_timeout" if timed_out else "drained",
                                after_s=round(now - info["since"], 3), lost_in_flight=pending_jobs)
                    self.in_flight[stage].clear()
                    finished.append(stage)
        if finished:
            self._publish_status()

    def _event(self, version, stage, event, **extra):
        record = {"t": time.time(), "site": self.site, "version": version, "stage": stage,
                  "event": event, **extra}
        with open(self.transitions_path, "a") as f:
            f.write(json.dumps(record) + "\n")
        log(f"[controller@{self.site}] v{version} {stage or '-'}: {event} {extra or ''}")

    # Sampling and observations

    def _sample(self):
        now = time.time()
        snapshot = self.backend.snapshot()  # stage -> desired/ready/alive/terminating/hpa
        cpu = self.backend.cpu_per_stage()  # stage -> mean CPU usage / limit of its pods
        with self.lock:
            counts = {s: snapshot[s]["desired"] for s in STAGES
                      if self.current is not None and self.current.site_of(s) == self.site
                      and s not in self.draining and snapshot[s]["desired"] > 0}
            counts_changed = counts != self.replica_counts
            self.replica_counts = counts
            for stage in STAGES:
                snap = snapshot[stage]
                ready, alive = snap["ready"], snap["alive"]
                placed = self.current is not None and self.current.site_of(stage) == self.site
                self.pods_csv.writerow([now, self.site, stage, int(placed), int(stage in self.draining),
                                        snap["desired"], ready, alive, snap["terminating"],
                                        int(snap["hpa"])])
                if placed or alive:
                    w = self.window[stage]
                    w.ready_samples.append(ready)
                    w.alive_samples.append(alive)
                    if stage in cpu:
                        w.cpu_samples.append(cpu[stage])
        self.pods_file.flush()
        if counts_changed:
            self._publish_status()

    def _observe(self):
        """Close the window and compute the state the agents expect.

        Per stage: the observation of the DQN agents (Pozzani's RLAgent.update_data /
        normalize_state). Per site: Lambda and utilization for the FIGARO master agent.
        """
        norm = self.config.get("normalization", {})
        now = time.time()
        with self.lock:
            elapsed = max(now - self.window_start, 1e-9)
            stages = {}
            for stage in STAGES:
                w = self.window[stage]
                placed = self.current is not None and self.current.site_of(stage) == self.site
                if not placed and not w.alive_samples:
                    continue
                stages[stage] = self._stage_observation(stage, w, elapsed, norm)
            lambda_rate = self.lambda_arrivals / elapsed if (
                self.current is not None and self.current.gateway == self.site) else None
            record = {
                "t": now,
                "site": self.site,
                "window_s": round(elapsed, 3),
                "placement_version": self.current.version if self.current else 0,
                "lambda": lambda_rate,
                "site_utilization": self.backend.site_utilization(),
                "stages": stages,
            }
            self.window = {s: StageWindow() for s in STAGES}
            self.window_start = now
            self.lambda_arrivals = 0
            # Only stages that are settled here: not the ones being drained or started.
            scalable = [s for s in stages if self.stage_cfg[s].get("agent")
                        and self.current is not None and self.current.site_of(s) == self.site
                        and s not in self.draining]
        # Outside the lock: an agent that is slow to answer must not block job tracking.
        for stage in scalable:
            stages[stage]["agent"] = self._ask_agent(stage, stages[stage]["observation"])
        with open(self.observations_path, "a") as f:
            f.write(json.dumps(record) + "\n")
        self.client.publish(multisite.observations_topic(self.site), json.dumps(record), qos=1)

    def _stage_observation(self, stage, w, elapsed, norm):
        mean = lambda xs: sum(xs) / len(xs) if xs else 0.0
        workload = w.arrivals / elapsed
        service_time = mean(w.service_times)
        response_time = mean(w.response_times)
        n_instances = mean(w.ready_samples)
        threshold = self.stage_cfg[stage].get("res_time_threshold", 0)
        theoretical_utilization = min(workload * service_time / n_instances, 1.0) if n_instances > 0 else 0.0
        raw = {
            "workload": workload,
            "service_time": service_time,
            "response_time": response_time,
            "n_instances": n_instances,
            "n_alive": mean(w.alive_samples),
            "docker_utilization": min(mean(w.cpu_samples), 1.0),
            "theoretical_utilization": theoretical_utilization,
            "pressure": response_time / threshold if threshold > 0 else 0.0,
            "queue_length_dominant": (response_time - service_time) / service_time if service_time > 0 else 0.0,
            "arrivals": w.arrivals,
            "completions": len(w.service_times),
        }
        # Same normalisation as ServerlessAutoscalingWithRL/figaro/controller/rlagent.py.
        max_workload = norm.get("max_workload", 0.25)
        max_n = norm.get("max_n_instances", 10)
        p_clip = norm.get("pressure_clip", 3)
        q_clip = norm.get("queue_length_clip", 10)
        observation = {
            "n_instances": raw["n_instances"] / max_n,
            "utilization": raw["theoretical_utilization"],
            "pressure": max(0.0, min(raw["pressure"], p_clip)) / p_clip,
            "queue_length_dominant": max(0.0, min(raw["queue_length_dominant"], q_clip)) / q_clip,
            "workload": min(1.0, raw["workload"] / max_workload),
        }
        return {"raw": raw, "observation": observation}

    def _ask_agent(self, stage, observation):
        """Send the observation to the stage's agent and apply the replicas it returns.

        On any error the current number of replicas is kept.
        """
        cfg = self.stage_cfg[stage]["agent"]
        result = {"action": None, "applied": None, "previous": self.backend.desired(stage), "error": None}
        try:
            request = urllib.request.Request(
                cfg["url"].rstrip("/") + "/action",
                data=json.dumps({"observation": observation}).encode(),
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=self.agent_timeout) as response:
                result["action"] = int(json.loads(response.read())["action"])
        except Exception as exc:
            result["error"] = repr(exc)
            log(f"[controller@{self.site}] {stage}: agent error, keeping {result['previous']} replicas: {exc!r}")
            return result
        low = cfg.get("min", 1)
        high = cfg.get("max", self.config.get("normalization", {}).get("max_n_instances", 10))
        replicas = max(low, min(result["action"], high))
        with self.lock:
            # The placement may have changed while waiting for the answer.
            still_here = (self.current is not None and self.current.site_of(stage) == self.site
                          and stage not in self.draining)
            if still_here:
                result["applied"] = replicas
                if replicas != result["previous"]:
                    self.backend.scale(stage, replicas)
                    log(f"[controller@{self.site}] {stage}: agent {result['previous']} -> {replicas} replicas")
        return result

    # Main loop

    def run(self, stop_event=None):
        stop_event = stop_event or threading.Event()
        multisite.connect(self.client, self.mqtt_host, self.mqtt_port)
        self.client.loop_start()
        next_observation = time.time() + self.observation_interval
        while not stop_event.is_set():
            started = time.time()
            try:
                self._check_ready()
                self._check_drains()
                self._sample()
                if started >= next_observation:
                    self._observe()
                    next_observation += self.observation_interval
            except Exception as exc:
                log(f"[controller@{self.site}] loop error: {exc!r}")
            stop_event.wait(max(0.0, self.sample_interval - (time.time() - started)))
        self.client.loop_stop()
        self.client.disconnect()
        self.pods_file.close()


def main():
    from kube_backend import KubeBackend

    site = os.environ["SITE"]
    with open(os.getenv("CONTROLLER_CONFIG", "/etc/controller/config.yaml")) as f:
        config = yaml.safe_load(f)
    backend = KubeBackend(os.getenv("NAMESPACE", "default"), config["stages"])
    controller = LocalController(
        site, config, backend,
        mqtt_host=os.getenv("MQTT_BROKER", "mosquitto-service"),
        mqtt_port=int(os.getenv("MQTT_PORT", "1883")),
        log_dir=os.getenv("LOG_DIR", "/var/log/multisite"),
    )
    controller.run()


if __name__ == "__main__":
    sys.exit(main())
