# import sys
import subprocess
import os
# import argparse

# from aisprint.annotations import annotation
from pipeline_worker import PipelineWorker

MODEL_PATH = "/opt/videosearcher/deepspeech-0.9.3-models.pbmm"
SCORER_PATH = "/opt/videosearcher/deepspeech-0.9.3-models.scorer"


def execute_command(command):
    print(">_ " + command)
    completed = subprocess.run(command.split(), capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(
            f"Command failed ({completed.returncode}): {command}\n"
            f"stdout: {completed.stdout}\n"
            f"stderr: {completed.stderr}"
        )


def get_command_output(command):
    print(">_ " + command)
    return subprocess.run(command.split(), capture_output=True, text=True)


def safe_remove(path):
    if os.path.exists(path):
        execute_command("rm " + path)


def deepspeech(i, dir):
    orig_input = f"{dir}/video_ffmpeg_2_output_{str(i)}.tar.gz"
    orig_output = f"{dir}/video_deepspeech_output_{str(i)}"

    input_dir = os.path.dirname(orig_input)
    output_dir = os.path.dirname(orig_output)
    output_name = os.path.basename(orig_output)

    archive_name = output_name + ".tar.gz"
    video_name = output_name + ".mp4"
    video_path = os.path.join(output_dir, video_name)

    command = "tar -xvzf %s -C %s" % (orig_input, input_dir)
    execute_command(command)

    model = MODEL_PATH
    scorer = SCORER_PATH
    audio_path = orig_input.replace(".tar.gz", ".wav")

    if not os.path.isfile(model):
        raise FileNotFoundError(f"DeepSpeech model file not found: {model}")
    if not os.path.isfile(scorer):
        raise FileNotFoundError(f"DeepSpeech scorer file not found: {scorer}")

    command = "deepspeech --model %s --scorer %s --audio %s" % (model, scorer, audio_path)
    # command = "whisper %s --model tiny --output_format txt" % (audio_path)
    output = get_command_output(command)
    if output.returncode != 0:
        raise RuntimeError(
            f"deepspeech command failed for {audio_path}: {output.stderr}"
        )
    transcription = output.stdout.strip()
    print(f"DEBUG: Transcription for {audio_path}: '{transcription}'")
    print(f"DEBUG: Stderr: {output.stderr}")
    # command = f"mv video_ffmpeg_2_output_{i}.txt {os.path.join(output_dir, 'transcript.txt')}"

    with open(os.path.join(output_dir, "transcript.txt"), "w") as file:
        file.write(transcription)

    execute_command("cp %s %s" % (orig_input.replace(".tar.gz", ".mp4"), video_path))
    os.chdir(output_dir)
    execute_command("tar -czvf %s %s %s" % (archive_name, video_name, "transcript.txt"))
    safe_remove(video_name)
    safe_remove("transcript.txt")
    safe_remove(audio_path)
    safe_remove(audio_path.replace(".wav", ".mp4"))


def handler(params, context):
    try:
        input_folder = f"/mnt/ramdisk/{params['dir']}"
        for file in os.listdir(input_folder):
            if "video_ffmpeg_2_output_" in file and file.endswith(".tar.gz"):
                i = file.split("_")[-1].split(".")[0]
                deepspeech(i, input_folder)
                safe_remove(os.path.join(input_folder, file.replace(".tar.gz", ".mp4")))
                safe_remove(os.path.join(input_folder, file.replace(".tar.gz", ".wav")))
    except Exception as e:
        f = open("/mnt/ramdisk/error.txt", "w")
        f.write(str(e))
        f.close()
        raise

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
         subscribe_topic = "pipeline/E",
         publish_topic   = "pipeline/F",
         group_name      = "groupE",
         handler_fn      = handler
     )
     worker.run()
