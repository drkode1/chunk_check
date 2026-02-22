import os
import time
import subprocess
import requests
import re

def get_video_duration(filename):
    cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', filename]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return float(result.stdout) if result.stdout.strip() else 0

def download_file(url, filename):
    print(f"--- Phase 1: Downloading ---")
    start_time = time.perf_counter()
    # Using a larger stream buffer for server-grade networking
    response = requests.get(url, stream=True, timeout=30)
    response.raise_for_status()
    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0
    
    with open(filename, 'wb') as f:
        # 16MB chunks often perform better on high-speed server links
        for chunk in response.iter_content(chunk_size=16*1024*1024):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    percent = (downloaded / total_size) * 100
                    print(f"\r{percent:.0f}% downloaded as of now", end="", flush=True)
    
    print("\nDownload Complete.")
    return time.perf_counter() - start_time

def process_video(input_file):
    print(f"--- Phase 2: Processing ---")
    total_duration = get_video_duration(input_file)
    output_folder = "stream_output"
    os.makedirs(output_folder, exist_ok=True)
    
    start_time = time.perf_counter()
    cmd = [
        'ffmpeg', '-i', input_file,
        '-codec:', 'copy',
        '-start_number', '0',
        '-hls_time', '10',
        '-hls_list_size', '0',
        '-f', 'hls', f'{output_folder}/index.m3u8',
        '-y'
    ]
    
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    time_pattern = re.compile(r"time=(\d+):(\d+):(\d+.\d+)")

    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None:
            break
        
        match = time_pattern.search(line)
        if match:
            hours, mins, secs = map(float, match.groups())
            current_pos = (hours * 3600) + (mins * 60) + secs
            if total_duration > 0:
                percent = (current_pos / total_duration) * 100
                print(f"\r{percent:.0f}% video processing as of now", end="", flush=True)

    if process.returncode == 0:
        print("\nProcessing Complete.")
        return time.perf_counter() - start_time
    return None

if __name__ == "__main__":
    url = os.getenv("DIRECT_DOWNLOAD_URL")
    file_name = "input_video.mp4"
    
    if not url:
        print("Error: DIRECT_DOWNLOAD_URL secret not found.")
    else:
        time_down = download_file(url, file_name)
        time_proc = process_video(file_name)
        
        if os.path.exists(file_name):
            os.remove(file_name)
            
        if time_proc:
            print(f"\n{time_down:.2f} {time_proc:.2f}")
