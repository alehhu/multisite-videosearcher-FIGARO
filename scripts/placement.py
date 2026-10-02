#!/usr/bin/env python3
"""Placement coordinator: changes where each stage runs, without losing jobs.

    placement.py publish placements/split-ffmpeg2.yaml --broker <cloud-broker>:1883
    placement.py show --broker <cloud-broker>:1883

`publish` runs the make-before-break protocol:
  1. wait until no site is still draining a previous change;
  2. publish the new table, retained, on control/placement/pending;
  3. wait until every site acks it on control/status/<site> (its new stages are ready);
  4. publish it, retained, on control/placement/current: from now on jobs follow it.
The local controllers then drain and stop the stages that left their site.

It needs only the cloud broker: the edge status reaches it through the bridge.
A placement agent can later take the place of this script.
"""

import argparse
import json
import os
import sys
import threading
import time

import yaml

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
import multisite  # noqa: E402
from multisite import SITES, Placement  # noqa: E402


class Coordinator:

    def __init__(self, host, port):
        self.current = None
        self.pending = None
        self.status = {}
        self.changed = threading.Condition()
        self.client = multisite.make_client()
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        multisite.connect(self.client, host, port)
        self.client.loop_start()
        # Retained messages arrive right after subscribing.
        time.sleep(1.5)

    def _on_connect(self, client, userdata, flags, rc, props=None):
        client.subscribe(multisite.PLACEMENT_TOPIC, qos=1)
        client.subscribe(multisite.PLACEMENT_PENDING_TOPIC, qos=1)
        client.subscribe(f"{multisite.STATUS_TOPIC_PREFIX}/+", qos=1)

    def _on_message(self, client, userdata, msg, props=None):
        with self.changed:
            if not msg.payload:
                pass
            elif msg.topic == multisite.PLACEMENT_TOPIC:
                self.current = Placement.from_json(msg.payload)
            elif msg.topic == multisite.PLACEMENT_PENDING_TOPIC:
                self.pending = Placement.from_json(msg.payload)
            else:
                status = json.loads(msg.payload)
                self.status[status["site"]] = status
            self.changed.notify_all()

    def wait_for(self, predicate, timeout, what):
        deadline = time.time() + timeout
        with self.changed:
            while not predicate():
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise TimeoutError(f"timed out after {timeout}s waiting for {what}; status={self.status}")
                self.changed.wait(min(remaining, 1.0))

    def publish(self, stages, gateway, timeout, wait_drained=True):
        if wait_drained:
            print("waiting for previous transitions to finish...")
            self.wait_for(lambda: all(not self.status.get(s, {}).get("draining") for s in SITES),
                          timeout, "sites to finish draining")
        with self.changed:
            last = max([0] + [p.version for p in (self.current, self.pending) if p])
        placement = Placement(last + 1, gateway, stages)
        v = placement.version

        print(f"v{v}: announcing {placement.stages} (gateway: {gateway})")
        self.client.publish(multisite.PLACEMENT_PENDING_TOPIC, placement.to_json(), qos=1, retain=True).wait_for_publish()
        t0 = time.time()
        self.wait_for(lambda: all(self.status.get(s, {}).get("acked_version", 0) >= v for s in SITES),
                      timeout, f"acks for v{v} from {SITES}")
        print(f"v{v}: all sites ready after {time.time() - t0:.1f}s, putting it in force")
        self.client.publish(multisite.PLACEMENT_TOPIC, placement.to_json(), qos=1, retain=True).wait_for_publish()
        return placement

    def show(self):
        with self.changed:
            print("current:", self.current.to_dict() if self.current else None)
            print("pending:", self.pending.to_dict() if self.pending else None)
            for site in SITES:
                print(f"status[{site}]:", self.status.get(site))


def load_placement_file(path):
    with open(path) as f:
        data = yaml.safe_load(f)
    return data["stages"], data.get("gateway", "edge")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["publish", "show"])
    parser.add_argument("file", nargs="?", help="placement YAML (publish)")
    parser.add_argument("--broker", default="localhost:1883", help="cloud broker host:port")
    parser.add_argument("--timeout", type=float, default=1800, help="seconds to wait at each step")
    parser.add_argument("--no-wait-drained", action="store_true",
                        help="do not wait for previous drains (debugging only)")
    args = parser.parse_args()

    host, _, port = args.broker.partition(":")
    coordinator = Coordinator(host, int(port or 1883))
    if args.command == "show":
        coordinator.show()
        return 0
    if not args.file:
        parser.error("publish needs a placement file")
    stages, gateway = load_placement_file(args.file)
    try:
        coordinator.publish(stages, gateway, args.timeout, wait_drained=not args.no_wait_drained)
    except TimeoutError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        coordinator.client.loop_stop()
        coordinator.client.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())
