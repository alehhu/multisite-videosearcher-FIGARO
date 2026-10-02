import subprocess
import time
import csv
import sys
import os
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

NAMESPACE = os.getenv("K8S_NAMESPACE", "default")
INTERVAL  = int(os.getenv("POD_POLL_INTERVAL", "10"))
OUTPUT    = sys.argv[1] if len(sys.argv) > 1 else "pod_counts.csv"

STAGES = {
    "ffmpeg0":    "ffmpeg0-deployment",
    "librosa":    "librosa-deployment",
    "ffmpeg1":    "ffmpeg1-deployment",
    "ffmpeg2":    "ffmpeg2-deployment",
    "deepspeech": "deepspeech-deployment",
    "grep":       "grep-deployment",
}

def query_stage(stage, deploy):
    try:
        ready = subprocess.check_output(
            ["kubectl", "get", "deployment", deploy, "-n", NAMESPACE,
             "-o", "jsonpath={.status.readyReplicas}"],
            timeout=5, text=True
        ).strip() or "0"
        desired = subprocess.check_output(
            ["kubectl", "get", "deployment", deploy, "-n", NAMESPACE,
             "-o", "jsonpath={.spec.replicas}"],
            timeout=5, text=True
        ).strip() or "0"
        return (stage, int(ready), int(desired))
    except Exception:
        return None

start = time.time()
with open(OUTPUT, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["timestamp", "time_from_start_s", "stage", "ready_replicas", "desired_replicas"])
    with ThreadPoolExecutor(max_workers=len(STAGES)) as pool:
        while True:
            ts = datetime.now(timezone.utc).isoformat()
            elapsed = round(time.time() - start, 1)
            futures = {pool.submit(query_stage, stage, deploy): stage
                       for stage, deploy in STAGES.items()}
            for fut in as_completed(futures):
                result = fut.result()
                if result:
                    stage, ready, desired = result
                    w.writerow([ts, elapsed, stage, ready, desired])
            f.flush()
            time.sleep(INTERVAL)
