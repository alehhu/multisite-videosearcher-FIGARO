import sys
import subprocess
import os
import json

import argparse
from pipeline_worker import PipelineWorker


def execute_command(command):
    print(">_ " + command)
    subprocess.run(command.split())


def grep(n, find_str, dir):
    filename = f"{dir}/video_deepspeech_output_{str(n)}.tar.gz"

    command = f"tar -xvzf {filename} -C {dir}/"
    execute_command(command)

    f = open(f"{dir}/transcript.txt", "r")
    content = f.read()
    if find_str in content:
        return filename

    execute_command(f"rm {dir}/transcript.txt")
    execute_command(f"rm {dir}/video_deepspeech_output_{str(n)}.mp4")

    return None


def handler(params, context):
    try:
        input_folder = f"/mnt/ramdisk/{params['dir']}"
        find_str = params.get('find_str', 'speech')

        for file in os.listdir(input_folder):
            if "video_deepspeech_output_" in file and file.endswith(".tar.gz"):
                i = file.split("_")[-1].split(".")[0]
                res = grep(i, find_str, input_folder)
                if res:
                    params['result'] = res
                    execute_command(f"rm -r {input_folder}/")
                    return params

        execute_command(f"rm -r {input_folder}/")
        params['result'] = 'No results found'

    except Exception as e:
        params['error'] = str(e)
        f = open("/mnt/ramdisk/error.txt", "w")
        f.write(str(e))
        f.close()

    return params


if __name__ == "__main__":
    # Stage, site and broker come from the environment (see common/pipeline_worker.py).
    PipelineWorker.from_env(handler).run()
