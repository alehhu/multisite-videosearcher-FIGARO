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
    # Stage, site and broker come from the environment (see common/pipeline_worker.py).
    PipelineWorker.from_env(handler).run()
