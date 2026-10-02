"""Stand-in for a pipeline function: same file chain as the real stages, no ffmpeg.

Each stage consumes `<job>/<previous stage>.out` and writes `<job>/<stage>.out`; ffmpeg0
creates the job directory, grep removes it. A job whose input file is missing fails,
so a broken seed or cleanup shows up as a DLQ message.
"""

import os
import shutil
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "common"))
from multisite import STAGES, previous_stage  # noqa: E402
from pipeline_worker import PipelineWorker  # noqa: E402

STAGE = os.environ["STAGE"]
SERVICE_S = float(os.getenv("FAKE_SERVICE_S", "0.2"))


def handler(params, context):
    job_dir = os.path.join(os.environ["RAMDISK"], params["dir"])
    prev = previous_stage(STAGE)
    if prev is None:
        os.makedirs(job_dir, exist_ok=True)
    else:
        input_file = os.path.join(job_dir, f"{prev}.out")
        if not os.path.exists(input_file):
            raise FileNotFoundError(input_file)
        os.remove(input_file)
    time.sleep(SERVICE_S)
    if STAGE == STAGES[-1]:
        shutil.rmtree(job_dir)
    else:
        with open(os.path.join(job_dir, f"{STAGE}.out"), "w") as f:
            f.write(params["dir"])
    return params


if __name__ == "__main__":
    PipelineWorker.from_env(handler).run()
