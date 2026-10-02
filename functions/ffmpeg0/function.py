import os
import subprocess # creates subprocesses of the program, similar to Java's threads.
from pipeline_worker import PipelineWorker

def execute_command(command): # private internal function
    print(">_ " + command)
    subprocess.run(command.split(), check=True) # subprocess library needs commands' words in an array form, where each of the word is an individual element of the array. The run() command constructs back the command by parsing each element of the array.
    # In this case we separate words with a blank space.

def handler(params, context):
    """
    logic that actually processes the file of the workflow.

    """


    input_dir = f"/mnt/ramdisk/{params['dir']}"
    os.makedirs(input_dir, exist_ok=True) # creates directory in case it doesn't exist already.

    print("ffmpeg_0 function has started...")


    def execute_command(command):
         print(">_ " + command)
         subprocess.run(command.split())

    input_file = 'video.mp4'
    input_name = input_file.split('.')[0]
    input_path = os.path.join('/mnt/ramdisk', input_file)
    output_name = f"{input_name}_ffmpeg_0_output"
    output_dir = input_dir

    print(f"SCRIPT: Input at '{input_path}', saving output in '{output_dir}/{output_name}'")

    archive_name = output_name + ".tar.gz"
    video_name = output_name + ".mp4"
    audio_name = output_name + ".wav"

    video_path = os.path.join(output_dir, video_name)
    audio_path = os.path.join(output_dir, audio_name)

    # Correct extraction: input is a video file, we extract audio and copy video
    execute_command("ffmpeg -y -i %s -map 0:a %s" % (input_path, audio_path))
    execute_command("cp %s %s" % (input_path, video_path))
    os.chdir(output_dir)
    execute_command("tar -czvf %s %s %s" % (archive_name, video_name, audio_name))
    execute_command("rm " + video_name)
    execute_command("rm " + audio_name)


    return params   # timestamps injected automatically by PipelineWorker


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
        subscribe_topic = "pipeline/A",
        publish_topic   = "pipeline/B",
        group_name      = "groupA",
        handler_fn      = handler
    )
    worker.run()
