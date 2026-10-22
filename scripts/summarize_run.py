#!/usr/bin/env python3
"""Summarise an experiment from the logs of the sites.

    summarize_run.py <logs-edge> [<logs-cloud>] [--duration 3600] [--start <epoch>]

Each argument is a copy of /var/log/multisite of one site. The run lasts `--duration`
seconds from its first job (or from `--start`): only what happens inside it is counted,
and the jobs still in the pipeline at the end are reported but are not an error.

It prints:
  - jobs: sent, completed, failed (DLQ), duplicated, still in the pipeline at the end;
  - per 5 minutes: jobs completed, job duration, and the wait of every stage (queueing,
    plus the hop from the previous stage), to see whether the queues keep growing;
  - the waits of the hops between the two sites against the local ones (bridge stalls);
  - the decisions of the scaling agents, and the replicas that stayed not ready;
and a final check of the conditions for the pipeline to have held.
"""

import argparse
import csv
import json
import os
import statistics
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
from multisite import STAGES  # noqa: E402


def read_jsonl(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def load(dirs):
    logs = {"timing": [], "observations": [], "pods": []}
    for d in dirs:
        logs["timing"] += read_jsonl(os.path.join(d, "timing.jsonl"))
        logs["observations"] += read_jsonl(os.path.join(d, "observations.jsonl"))
        logs["pods"] += read_csv(os.path.join(d, "pods.csv"))
    return logs


def percentile(values, q):
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(len(values) - 1, int(round(q * (len(values) - 1))))]


def mean(values):
    return statistics.mean(values) if values else 0.0


def jobs_summary(timing, observations, start, end):
    """Counts of the jobs of the run. `sent` comes from the gateway's arrivals (lambda)."""
    sent = round(sum(o["lambda"] * o["window_s"] for o in observations
                     if o.get("lambda") is not None and o["t"] > start and o["t"] - o["window_s"] < end))
    seen, completed, failed, duplicates, late = set(), [], [], 0, 0
    for record in timing:
        if record["job_id"] in seen:
            duplicates += 1
            continue
        seen.add(record["job_id"])
        if record["logged_at"] > end:
            late += 1
        elif record.get("error") or record["topic"].endswith("/dlq"):
            failed.append(record)
        else:
            completed.append(record)
    return {"sent": sent, "completed": completed, "failed": failed, "duplicates": duplicates,
            "after_end": late, "in_pipeline_at_end": max(0, sent - len(completed) - len(failed))}


def job_duration(record):
    ts = record["timestamps"]
    return record["stages"][-1]["end"] - ts["gateway_received"]


def per_bucket(completed, start, end, bucket_s):
    """Rows (from_min, jobs, mean duration, p95 duration, {stage: mean wait}) by arrival time."""
    rows = []
    t = start
    while t < end:
        jobs = [r for r in completed if t <= r["timestamps"]["gateway_received"] < t + bucket_s]
        durations = [job_duration(r) for r in jobs]
        waits = {stage: mean([s["wait"] for r in jobs for s in r["stages"]
                              if s["stage"] == stage and "wait" in s]) for stage in STAGES}
        rows.append(((t - start) / 60, len(jobs), mean(durations), percentile(durations, 0.95), waits))
        t += bucket_s
    return rows


def hop_waits(completed):
    """Waits of the hops into each stage, split into local hops and hops between sites."""
    local, remote = [], []
    for record in completed:
        previous = record["sites"].get("gateway")
        for s in record["stages"]:
            if "wait" in s:
                (local if s.get("site") == previous else remote).append(s["wait"])
            previous = s.get("site")
    return local, remote


def agent_decisions(observations, start, end):
    """{(site, stage): [(minute, previous, action, applied, error)]} inside the run."""
    decisions = {}
    for o in sorted(observations, key=lambda o: o["t"]):
        if not start <= o["t"] <= end:
            continue
        for stage, state in o["stages"].items():
            agent = state.get("agent")
            if agent is not None:
                decisions.setdefault((o["site"], stage), []).append(
                    ((o["t"] - start) / 60, agent["previous"], agent["action"], agent["applied"], agent["error"]))
    return decisions


def not_ready_stretches(pods, start, end):
    """{(site, stage): (longest, total)} seconds with fewer ready replicas than desired,
    while the stage was placed on that site."""
    by_key = {}
    for row in pods:
        t = float(row["timestamp"])
        if start <= t <= end and row["placed_here"] == "1":
            by_key.setdefault((row["site"], row["stage"]), []).append((t, int(row["desired"]) > int(row["ready"])))
    result = {}
    for key, samples in by_key.items():
        samples.sort()
        longest = total = 0.0
        since = None
        for (t, short), (t_next, _) in zip(samples, samples[1:] + [(samples[-1][0], False)]):
            if short:
                since = t if since is None else since
                total += t_next - t
                longest = max(longest, t_next - since)
            else:
                since = None
        result[key] = (longest, total)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("dirs", nargs="+", help="copies of /var/log/multisite, one per site")
    parser.add_argument("--duration", type=float, default=3600, help="seconds of the run (default: %(default)s)")
    parser.add_argument("--start", type=float, help="epoch of the start (default: the first job)")
    parser.add_argument("--bucket", type=float, default=300, help="seconds per row of the table (default: %(default)s)")
    parser.add_argument("--max-not-ready", type=float, default=120,
                        help="longest time a stage may have replicas not ready (default: %(default)s s)")
    args = parser.parse_args()

    logs = load(args.dirs)
    if not logs["timing"]:
        print("No timing.jsonl found: pass the logs of the gateway's site.", file=sys.stderr)
        return 1
    start = args.start or min(r["timestamps"]["gateway_received"] for r in logs["timing"])
    end = start + args.duration

    jobs = jobs_summary(logs["timing"], logs["observations"], start, end)
    completed = jobs["completed"]
    print(f"Run of {args.duration / 60:.0f} minutes")
    print(f"\nJobs: {jobs['sent']} sent, {len(completed)} completed, {len(jobs['failed'])} failed (DLQ), "
          f"{jobs['duplicates']} duplicated, {jobs['in_pipeline_at_end']} still in the pipeline at the end"
          + (f" ({jobs['after_end']} of them finished later)" if jobs["after_end"] else ""))
    for record in jobs["failed"]:
        print(f"  failed {record['job_id']}: {record.get('error')}")

    print(f"\nBy arrival time (job duration and mean wait per stage, seconds):")
    header = f"{'from min':>8} {'jobs':>5} {'mean':>7} {'p95':>7}  " + " ".join(f"{s:>10}" for s in STAGES)
    print(header)
    for minute, n, avg, p95, waits in per_bucket(completed, start, end, args.bucket):
        print(f"{minute:>8.0f} {n:>5} {avg:>7.1f} {p95:>7.1f}  " + " ".join(f"{waits[s]:>10.2f}" for s in STAGES))

    local, remote = hop_waits(completed)
    print("\nWait of the hops (seconds):")
    for name, values in (("local", local), ("between sites", remote)):
        if values:
            print(f"  {name:<14} n={len(values):<5} median={statistics.median(values):.3f} "
                  f"p95={percentile(values, 0.95):.3f} max={max(values):.3f}")

    decisions = agent_decisions(logs["observations"], start, end)
    errors = 0
    print("\nAgents (minute: replicas before -> applied):")
    if not decisions:
        print("  none")
    for (site, stage), items in sorted(decisions.items()):
        changes = [f"{m:.0f}: {prev}->{applied}" for m, prev, action, applied, error in items
                   if applied is not None and applied != prev]
        failed = [m for m, prev, action, applied, error in items if error]
        errors += len(failed)
        print(f"  {stage}@{site}: {len(items)} decisions, {len(changes)} changes"
              + (f", {len(failed)} without answer" if failed else "")
              + (f"  [{', '.join(changes)}]" if changes else ""))

    stretches = not_ready_stretches(logs["pods"], start, end)
    slow = {k: v for k, v in stretches.items() if v[0] > args.max_not_ready}
    print(f"\nReplicas not ready for more than {args.max_not_ready:.0f} s in a row:")
    if not slow:
        print("  none")
    for (site, stage), (longest, total) in sorted(slow.items()):
        print(f"  {stage}@{site}: longest {longest:.0f} s, {total:.0f} s in all")

    checks = [
        ("no failed job", not jobs["failed"]),
        ("no duplicated job", jobs["duplicates"] == 0),
        ("every agent answered", errors == 0),
        ("replicas became ready in time", not slow),
    ]
    print("\nChecks:")
    for name, ok in checks:
        print(f"  {'ok  ' if ok else 'FAIL'} {name}")
    return 0 if all(ok for _, ok in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
