import sys
import subprocess
import os

import argparse

from aisprint.annotations import annotation
from pipeline_worker import PipelineWorker


def execute_command(command):
    print(">_ " + command)
    subprocess.run(command.split())

def handler(params, context):
    try:
        input_dir = f"/mnt/ramdisk/{params['dir']}"

        input_file = 'video.mp4'
        input_name = input_file.split('.')[0]
        input_path = f"{input_dir}/{input_name}_librosa_output.tar.gz"
        output_path = f"{input_dir}/{input_name}_ffmpeg_1_output"

        command = "tar -xvzf %s -C %s" % (input_path, input_dir)
        execute_command(command)

        timestamp_path = os.path.join(input_dir, "timestamps.txt")
        video_path = input_path.replace(".tar.gz", ".mp4")

        with open(timestamp_path) as file:
            lines = file.readlines()

        files = []
        for i in range(len(lines)):
            line = lines[i]
            start, end = line.split()

            clip_path = output_path + "_" + str(i) + ".mp4"
            files.append(f"{input_name}_ffmpeg_1_output_{i}.mp4")

            command = "ffmpeg -y -ss %s -to %s -i %s -c copy %s" % (start, end, video_path, clip_path)
            execute_command(command)


        os.chdir(input_dir)
        archive_name = f"{input_name}_ffmpeg_1_output.tar.gz"
        command = "tar -czvf %s %s" % (archive_name, " ".join(files))
        execute_command(command)

        for i in range(len(lines)):
            clip_path = output_path + "_" + str(i) + ".mp4"
            execute_command("rm " + clip_path)

        execute_command("rm " + video_path)
        execute_command("rm " + timestamp_path)

    except Exception as e:
        ffff = open("/mnt/ramdisk/exc.txt", "w")
        ffff.write(str(e))
        ffff.close()

    return params



if __name__ == "__main__":

     """
     A detail that is worth reporting, is that normally in publisher-       subscribers systems, once a publisher sends a message or event, all the    subscribers are being notified. That creates ambiguity for which of the    subscribers actually takes on the job.
     It is for this reason that we use MQTT v5, and in particular its       "Shared Subscription" feature.
     Basically, in our experiment pods that belong to the same function     are being treated by Mosquitto as a single entity. Once it notifies that   single entity, there is a built-in algorithm that assignes the job to      just an individual subscriber inside that subscriber group (which          represents an individual function.
     We want to avoid indeed replicas of the same deployment to take on     the same job, wasting obviously resources.


     The individual application (which is going to have multiple instances  of it).

     """
     worker = PipelineWorker(
         broker_host     = "mosquitto-service",   # K8s ClusterIP Service   name
         broker_port     = 1883,
         subscribe_topic = "pipeline/C",
         publish_topic   = "pipeline/D",
         group_name      = "groupC",
         handler_fn      = handler
     )
     worker.run()
