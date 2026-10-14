#!/usr/bin/env python3
"""Measure the service time of every stage: N jobs sent one after the other, so that no
job waits in a queue.

    profile_stages.py --gateway <edge-host>:30080 [-n 20] [--warmup 1] [--csv out.csv]

Each job is sent synchronously to the gateway and the next one leaves only when it is
back. The per-stage times come from the timestamps in the reply (end - start of each
stage), with the site that ran it, as set by the current placement. The first `--warmup`
jobs are left out of the summary: the first run of a stage on a fresh pod is slower.

The summary also shows the overhead of each job: its duration at the gateway minus the
sum of the stages (broker hops, bridge, and up to 0.1 s of polling in the gateway).
"""

import argparse
import csv
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
from multisite import GROUPS, STAGES  # noqa: E402

WORKFLOW = "videosearcher_workflow"


def send_job(gateway, timeout):
    """Run one job and return the gateway's reply (a dict), also for failed jobs."""
    request = urllib.request.Request(
        f"http://{gateway}/workflow/invoke/{WORKFLOW}",
        data=json.dumps({"params": {}, "Async": False}).encode(),
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:  # failed jobs come back as 500 with a JSON body
        try:
            return json.loads(exc.read())
        except ValueError:
            return {"status": "error", "error": f"HTTP {exc.code}"}
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        return {"status": "error", "error": repr(exc)}


def parse_job(reply):
    """Per-stage times of a successful job: {"stages": {stage: (site, seconds)}, "total", "overhead"}."""
    result = reply["result"]
    ts = result["timestamps"]
    sites = result.get("sites", {})
    stages = {}
    for stage in STAGES:
        group = GROUPS[stage]
        start, end = ts.get(f"{group}_start"), ts.get(f"{group}_end")
        if start is None or end is None:
            raise ValueError(f"no timestamps for {stage}")
        stages[stage] = (sites.get(stage, "?"), float(end) - float(start))
    total = float(reply["Reports"]["Gateway"]["Duration"])
    return {"job_id": reply.get("job_id"), "stages": stages, "total": total,
            "overhead": total - sum(seconds for _, seconds in stages.values())}


def describe(values):
    std = statistics.stdev(values) if len(values) > 1 else 0.0
    return statistics.mean(values), std, min(values), max(values)


def summary_rows(jobs):
    """One row per stage, then the job total and the overhead: (name, site, n, mean, std, min, max)."""
    rows = []
    for stage in STAGES:
        values = [job["stages"][stage][1] for job in jobs]
        sites = sorted({job["stages"][stage][0] for job in jobs})
        rows.append((stage, "+".join(sites), len(values), *describe(values)))
    for key, name in (("total", "job total"), ("overhead", "overhead")):
        values = [job[key] for job in jobs]
        rows.append((name, "", len(values), *describe(values)))
    return rows


def print_summary(rows):
    print(f"\n{'stage':<11} {'site':<10} {'n':>3} {'mean':>8} {'std':>7} {'min':>8} {'max':>8}   (seconds)")
    for name, site, n, mean, std, low, high in rows:
        print(f"{name:<11} {site:<10} {n:>3} {mean:>8.3f} {std:>7.3f} {low:>8.3f} {high:>8.3f}")


def write_csv(path, jobs):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["job", "job_id", "stage", "site", "seconds"])
        for i, job in enumerate(jobs, 1):
            for stage, (site, seconds) in job["stages"].items():
                writer.writerow([i, job["job_id"], stage, site, f"{seconds:.4f}"])
            writer.writerow([i, job["job_id"], "job total", "", f"{job['total']:.4f}"])


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--gateway", required=True, help="host:port of the edge gateway")
    parser.add_argument("-n", type=int, default=20, help="jobs to measure (default: %(default)s)")
    parser.add_argument("--warmup", type=int, default=1,
                        help="jobs sent first and left out of the summary (default: %(default)s)")
    parser.add_argument("--timeout", type=float, default=900, help="seconds to wait for one job")
    parser.add_argument("--csv", help="write the measured jobs, one row per stage")
    args = parser.parse_args()

    measured, failed = [], 0
    for i in range(1, args.warmup + args.n + 1):
        warmup = i <= args.warmup
        label = f"warmup {i}/{args.warmup}" if warmup else f"job {i - args.warmup}/{args.n}"
        t0 = time.time()
        reply = send_job(args.gateway, args.timeout)
        if reply.get("status") != "success":
            failed += 1
            print(f"{label}: FAILED after {time.time() - t0:.1f}s: {reply.get('error') or reply}", flush=True)
            continue
        try:
            job = parse_job(reply)
        except (KeyError, TypeError, ValueError) as exc:
            failed += 1
            print(f"{label}: unexpected reply ({exc!r})", flush=True)
            continue
        sites = sorted({site for site, _ in job["stages"].values()})
        print(f"{label}: {job['total']:.2f}s on {'+'.join(sites)}"
              + ("  (not counted)" if warmup else ""), flush=True)
        if not warmup:
            measured.append(job)

    if failed:
        print(f"\n{failed} job(s) failed")
    if not measured:
        print("No job measured.", file=sys.stderr)
        return 1
    print_summary(summary_rows(measured))
    if args.csv:
        write_csv(args.csv, measured)
        print(f"\n{len(measured)} jobs written to {args.csv}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
