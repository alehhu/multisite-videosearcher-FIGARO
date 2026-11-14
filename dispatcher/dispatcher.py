"""Dispatcher: one per site, hands each job to a worker that is free.

Mosquitto gives a shared-subscription message to one subscriber as soon as it is
published, busy or not: the queue builds up inside the replicas that exist, and a
replica started later only gets the jobs published after it, while the old ones work
through their backlog. Serverledge, where the agents were trained, keeps one queue per
node and gives the next request to the first free container. This does the same, with
one FIFO queue per stage:

  - it is the only subscriber of pipeline/<site>/<stage>, for every stage, so every job
    entering a stage on this site waits here;
  - a worker announces on work/<site>/<stage>/state that it is idle (after starting and
    after every job), or that it leaves (on SIGTERM, after its last job); its last will
    tells that it was lost;
  - the first job in the queue goes to the first idle worker, on that worker's own topic
    work/<site>/<stage>/to/<worker>.

A job is acknowledged to the broker only once it has been handed to a worker, under a
persistent session: if the dispatcher restarts, the jobs it held come back from the
broker. The job of a worker lost (or leaving without having taken it) is published
again on its stage topic, so that the broker keeps it too: it goes to the back of the
queue, and the job message counts it in `requeued[<stage>]`. Not covered: a worker lost
while the dispatcher is down (its job is gone).

Each job gets two timestamps per stage: <group>_queued when it reaches the dispatcher,
<group>_dispatched when it is handed to a worker.
"""

import collections
import json
import os
import queue
import time

import multisite
from multisite import GROUPS, STAGES

# Messages the broker may send to the dispatcher before it acknowledges them: every job
# waiting here is one of them. MQTT's maximum.
RECEIVE_MAXIMUM = 65535
# Keep the session (subscriptions and the jobs for the dispatcher) while it is away.
SESSION_EXPIRY_S = 0xFFFFFFFF


def log(*args):
    print(time.strftime("[%Y-%m-%d %H:%M:%S]"), "[dispatcher]", *args, flush=True)


class Board:
    """The queues and the workers of every stage of one site. No MQTT: easy to test.

    A job is a dict (the job message) with an optional `token`, what is needed to
    acknowledge it to the broker (None once acknowledged).
    """

    def __init__(self):
        self.waiting = {s: collections.deque() for s in STAGES}   # (params, token)
        self.idle = {s: collections.deque() for s in STAGES}      # worker ids, in order
        self.busy = {s: {} for s in STAGES}                       # worker -> params
        self.requeued = []                                        # (stage, params) to publish again

    def add_job(self, stage, params, token=None):
        self.waiting[stage].append((params, token))

    def _requeue(self, stage, params):
        requeued = params.setdefault("requeued", {})
        requeued[stage] = requeued.get(stage, 0) + 1
        self.requeued.append((stage, params))

    def take_requeued(self):
        result, self.requeued = self.requeued, []
        return result

    def worker_state(self, stage, worker, state, last_job=None):
        """Apply what a worker said. Returns what happened, for the log."""
        busy_with = self.busy[stage].get(worker)
        if state == "idle":
            if busy_with is not None and busy_with.get("dir") != last_job:
                # Said before it got the job we sent it (e.g. answering a hello): stale.
                return "stale"
            self.busy[stage].pop(worker, None)
            if worker not in self.idle[stage]:
                self.idle[stage].append(worker)
            return "idle"
        if state in ("leaving", "lost"):
            if worker in self.idle[stage]:
                self.idle[stage].remove(worker)
            self.busy[stage].pop(worker, None)
            # Leaving: the worker finished its last job, the one sent after it is not done.
            # Lost: whatever it had is not done.
            if busy_with is not None and (state == "lost" or busy_with.get("dir") != last_job):
                self._requeue(stage, busy_with)
                return f"{state}, job {busy_with.get('dir')} published again"
            return state
        return "unknown"

    def assignments(self, stage):
        """Pair the first waiting jobs with the first idle workers: [(worker, params, token)]."""
        result = []
        while self.waiting[stage] and self.idle[stage]:
            params, token = self.waiting[stage].popleft()
            worker = self.idle[stage].popleft()
            self.busy[stage][worker] = params
            result.append((worker, params, token))
        return result

    def drop_waiting(self):
        """After a reconnection the broker sends again the jobs not acknowledged yet."""
        dropped = sum(len(q) for q in self.waiting.values())
        for stage in STAGES:
            self.waiting[stage].clear()
        return dropped

    def summary(self):
        return {s: (len(self.waiting[s]), len(self.idle[s]), len(self.busy[s]))
                for s in STAGES
                if self.waiting[s] or self.idle[s] or self.busy[s]}


class Dispatcher:

    def __init__(self, site, broker_host=None, broker_port=None, report_s=60):
        self.site = site
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.report_s = report_s
        self.board = Board()
        self.events = queue.Queue()
        self.job_topics = {multisite.stage_topic(site, s): s for s in STAGES}
        self.state_topics = {multisite.worker_state_topic(site, s): s for s in STAGES}
        self.client = multisite.make_client(client_id=f"dispatcher-{site}", manual_ack=True)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    # MQTT callbacks (network thread): everything is handled on the main thread.

    def _on_connect(self, client, userdata, flags, rc, props=None):
        for topic in list(self.job_topics) + list(self.state_topics):
            client.subscribe(topic, qos=1)
        self.events.put(("connected", None))

    def _on_message(self, client, userdata, msg, props=None):
        self.events.put(("message", msg))

    # Main thread

    def _handle(self, kind, msg):
        if kind == "connected":
            dropped = self.board.drop_waiting()
            log(f"connected to the broker of {self.site}"
                + (f", {dropped} jobs not acknowledged will come again" if dropped else ""))
            self.client.publish(multisite.dispatcher_hello_topic(self.site), b"{}", qos=1)
            return
        if msg.topic in self.job_topics:
            self._on_job(self.job_topics[msg.topic], msg)
        elif msg.topic in self.state_topics:
            self._on_state(self.state_topics[msg.topic], msg)
        else:
            self.client.ack(msg.mid, msg.qos)

    def _on_job(self, stage, msg):
        try:
            params = json.loads(msg.payload.decode())
        except ValueError:
            params = None
        if not isinstance(params, dict) or not params.get("dir"):
            log(f"dropping a malformed job message for {stage}")
            self.client.ack(msg.mid, msg.qos)
            return
        params.setdefault("timestamps", {})[f"{GROUPS[stage]}_queued"] = time.time()
        self.board.add_job(stage, params, token=(msg.mid, msg.qos))
        self._dispatch(stage)

    def _on_state(self, stage, msg):
        self.client.ack(msg.mid, msg.qos)
        try:
            data = json.loads(msg.payload.decode())
            worker, state = data["worker"], data["state"]
        except (ValueError, KeyError, TypeError):
            log(f"ignoring a malformed state message for {stage}")
            return
        what = self.board.worker_state(stage, worker, state, data.get("last_job"))
        if what not in ("idle", "stale"):
            log(f"{stage}: worker {worker} {what}")
        for requeued_stage, params in self.board.take_requeued():
            self.client.publish(multisite.stage_topic(self.site, requeued_stage), json.dumps(params), qos=1)
        self._dispatch(stage)

    def _dispatch(self, stage):
        for worker, params, token in self.board.assignments(stage):
            params["timestamps"][f"{GROUPS[stage]}_dispatched"] = time.time()
            # Same connection: the broker gets the job for the worker before the ack.
            self.client.publish(multisite.work_topic(self.site, stage, worker),
                                json.dumps(params), qos=1)
            if token is not None:
                self.client.ack(*token)

    def run(self, stop_event=None):
        multisite.connect(self.client, self.broker_host, self.broker_port,
                          receive_maximum=RECEIVE_MAXIMUM, session_expiry=SESSION_EXPIRY_S)
        self.client.loop_start()
        next_report = time.time() + self.report_s
        try:
            while stop_event is None or not stop_event.is_set():
                try:
                    kind, msg = self.events.get(timeout=0.5)
                    self._handle(kind, msg)
                except queue.Empty:
                    pass
                if time.time() >= next_report:
                    next_report += self.report_s
                    state = self.board.summary()
                    if state:
                        log("waiting/idle/busy: " + ", ".join(
                            f"{s} {w}/{i}/{b}" for s, (w, i, b) in state.items()))
        finally:
            self.client.disconnect()
            self.client.loop_stop()


def main():
    site = os.environ["SITE"]
    Dispatcher(site, os.getenv("MQTT_BROKER", "localhost"), int(os.getenv("MQTT_PORT", "1883")),
               report_s=float(os.getenv("REPORT_S", "60"))).run()


if __name__ == "__main__":
    main()
