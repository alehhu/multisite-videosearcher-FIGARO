#!/usr/bin/env python3
"""Measure the MQTT round-trip time between the two sites, through the bridge.

    link_probe.py --broker <edge-host>:31883 --from edge --to cloud [-n 200] [--interval 0.2] [--csv out.csv]

Publishes probes on probe/<to>/ping of the `--from` broker; the local controller of
`--to` echoes each one back on probe/<from>/pong. With --to equal to --from the probe
never leaves the broker: that is the local baseline to subtract.

Compare the cross-site RTT with a plain `ping` between the two servers: the difference is
the cost of the bridge and of the path (e.g. the SSH tunnel through the jump hosts).
One-way delays are printed too, but they are only meaningful with synchronised clocks.
"""

import argparse
import csv
import json
import os
import statistics
import sys
import threading
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
import multisite  # noqa: E402
from multisite import SITES  # noqa: E402


def percentile(values, p):
    values = sorted(values)
    k = (len(values) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (k - lo)


def summary(name, values_ms):
    if not values_ms:
        return f"{name}: no samples"
    return (f"{name}: n={len(values_ms)} min={min(values_ms):.2f} median={statistics.median(values_ms):.2f} "
            f"p95={percentile(values_ms, 0.95):.2f} max={max(values_ms):.2f} "
            f"mean={statistics.mean(values_ms):.2f} ms")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--broker", required=True, help="host:port of the --from site's broker")
    parser.add_argument("--from", dest="src", choices=SITES, required=True)
    parser.add_argument("--to", dest="dst", choices=SITES, required=True)
    parser.add_argument("-n", type=int, default=200, help="number of probes")
    parser.add_argument("--interval", type=float, default=0.2, help="seconds between probes")
    parser.add_argument("--timeout", type=float, default=10, help="seconds to wait for late replies")
    parser.add_argument("--csv", help="write one row per probe")
    args = parser.parse_args()

    run_id = uuid.uuid4().hex[:8]
    sent, received = {}, {}
    done = threading.Event()
    lock = threading.Lock()

    def on_connect(client, userdata, flags, rc, props=None):
        client.subscribe(multisite.probe_pong_topic(args.src), qos=1)

    def on_message(client, userdata, msg, props=None):
        now = time.time()
        probe = json.loads(msg.payload)
        if probe.get("run") != run_id:
            return
        with lock:
            received[probe["seq"]] = (now, probe["echoed_at"])
            if len(received) == args.n:
                done.set()

    host, _, port = args.broker.partition(":")
    client = multisite.make_client()
    client.on_connect = on_connect
    client.on_message = on_message
    multisite.connect(client, host, int(port or 1883))
    client.loop_start()
    time.sleep(1)

    print(f"probing {args.src} -> {args.dst} -> {args.src} via {args.broker}, {args.n} probes")
    for seq in range(args.n):
        t = time.time()
        with lock:
            sent[seq] = t
        payload = {"run": run_id, "seq": seq, "sent_at": t, "reply_to": multisite.probe_pong_topic(args.src)}
        client.publish(multisite.probe_ping_topic(args.dst), json.dumps(payload), qos=1)
        time.sleep(args.interval)
    done.wait(args.timeout)
    client.loop_stop()
    client.disconnect()

    rtt, out, back, rows = [], [], [], []
    for seq, t_sent in sent.items():
        if seq in received:
            t_recv, t_echo = received[seq]
            rtt.append((t_recv - t_sent) * 1000)
            out.append((t_echo - t_sent) * 1000)
            back.append((t_recv - t_echo) * 1000)
            rows.append([seq, t_sent, t_echo, t_recv])
        else:
            rows.append([seq, t_sent, "", ""])

    print(summary("round trip", rtt))
    print(summary(f"one way {args.src}->{args.dst} (needs synced clocks)", out))
    print(summary(f"one way {args.dst}->{args.src} (needs synced clocks)", back))
    lost = args.n - len(rtt)
    if lost:
        print(f"lost: {lost} probes (is the {args.dst} local controller running, is the bridge up?)")
    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["seq", "sent_at", "echoed_at", "received_at"])
            w.writerows(rows)
    return 0 if not lost else 1


if __name__ == "__main__":
    sys.exit(main())
