import sys
import subprocess
import os
import argparse

from aisprint.annotations import annotation
from pipeline_worker import PipelineWorker


def execute_command(command):
    print(">_ " + command)
    subprocess.run(command.split())


def ffmpeg_2(orig_input, i, folder):
    output_dir = folder
    output_name = f"video_ffmpeg_2_output_{str(i)}"
    orig_output = os.path.join(output_dir, output_name)

    temp_audio_path = orig_input.replace(".mp4", ".wav")
    output_audio_path = orig_output + ".wav"
    output_clip_path = orig_output + ".mp4"
    output_zip_path = orig_output + ".tar.gz"

    output_audio_name = output_name + ".wav"
    output_clip_name = output_name + ".mp4"
    output_zip_name = output_name + ".tar.gz"

    # extract audio track
    execute_command("ffmpeg -y -i %s -map 0:a %s" % (orig_input, temp_audio_path))

    # down-sample audio track
    execute_command("ffmpeg -y -i %s -vn -ar 16000 -ac 1 %s" % (temp_audio_path, output_audio_path))

    # compress video file
    execute_command("ffmpeg -y -i %s -vcodec libx265 -crf 30 %s" % (orig_input, output_clip_path))

    # alternative that copies the clip without compression
    # cp "$INPUT_FILE_PATH" "$TMP_OUTPUT_DIR/$INPUT_FILE"

    # repack everything, remove unzipped outputs
    os.chdir(output_dir)
    execute_command("tar -czvf %s %s %s" % (output_zip_name, output_audio_name, output_clip_name))
    execute_command("rm " + output_audio_name)
    execute_command("rm " + output_clip_name)

    execute_command("rm " + temp_audio_path)
    execute_command("rm " + orig_input)


def handler(params, context):
    folder = f"/mnt/ramdisk/{params['dir']}"
    input_file = f"{folder}/video_ffmpeg_1_output.tar.gz"

    # extract the input file
    os.chdir(folder)
    command = "tar -xvzf %s" % (input_file)
    execute_command(command)

    input_files = []
    files = os.listdir(folder)
    for file in files:
        if "ffmpeg_1_output_" in file:
            input_files.append(os.path.join(folder, file))

    for file in input_files:
        i = file.split("_")[-1].split(".")[0]
        ffmpeg_2(file, i, folder)


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
         subscribe_topic = "pipeline/D",
         publish_topic   = "pipeline/E",
         group_name      = "groupD",
         handler_fn      = handler
     )
     worker.run()

