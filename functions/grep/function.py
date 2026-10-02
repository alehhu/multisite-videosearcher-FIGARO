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

    """
    A detail that is worth reporting, is that normally in publisher-subscribers systems, once a publisher sends a message or event, all the subscribers are being notified. That creates ambiguity for which of the subscribers actually takes on the job.
    It is for this reason that we use MQTT v5, and in particular its "Shared Subscription" feature.
    Basically, in our experiment pods that belong to the same function are being treated by Mosquitto as a single entity. Once it notifies that single entity, there is a built-in algorithm that assignes the job to just an individual subscriber inside that subscriber group (which represents an individual function.
    We want to avoid indeed replicas of the same deployment to take on the same job, wasting obviously resources.


    The individual application (which is going to have multiple instances of it).

    """
    worker = PipelineWorker(
        broker_host     = "mosquitto-service",   # K8s ClusterIP Service name
        broker_port     = 1883,
        subscribe_topic = "pipeline/F",
        publish_topic   = "pipeline/done",
        group_name      = "groupF",
        handler_fn      = handler
    )
    worker.run()
