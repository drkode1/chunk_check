import os
import time
import subprocess
import requests
import re

def get_video_duration(filename):
    cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', filename]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        return float(result.stdout.strip())
    except:
        return 0

def download_file(url, filename):
    print(f"--- Phase 1: Downloading ---")
    start_time = time.perf_counter()
    
    # Using a session and larger stream buffer
    with requests.Session() as session:
        response = session.get(url, stream=True, timeout=60)
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        last_reported = -1
        
        with open(filename, 'wb') as f:
            # Increased to 32MB chunks for faster I/O in cloud runners
            for chunk in response.iter_content(chunk_size=32*1024*1024):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        percent = int((downloaded / total_size) * 100)
                        if percent > last_reported:
                            print(f"{percent}% downloaded as of now")
                            last_reported = percent
    
    print("Download Complete.")
    return time.perf_counter() - start_time

def process_video(input_file):
    print(f"--- Phase 2: Processing ---")
    total_duration = get_video_duration(input_file)
    output_folder = "stream_output"
    os.makedirs(output_folder, exist_ok=True)
    
    start_time = time.perf_counter()
    
    # Removed colon from -codec for correct FFmpeg syntax
    cmd = [
        'ffmpeg', '-i', input_file,
        '-codec', 'copy', 
        '-start_number', '0',
        '-hls_time', '10',
        '-hls_list_size', '0',
        '-f', 'hls', f'{output_folder}/index.m3u8',
        '-y'
    ]
    
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    print("FFmpeg engine started...")
    
    time_pattern = re.compile(r"time=(\d+):(\d+):(\d+.\d+)")
    last_reported = -1

    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None:
            break
        
        match = time_pattern.search(line)
        if match:
            hours, mins, secs = map(float, match.groups())
            current_pos = (hours * 3600) + (mins * 60) + secs
            if total_duration > 0:
                percent = int((current_pos / total_duration) * 100)
                if percent > last_reported:
                    print(f"{percent}% video processing as of now")
                    last_reported = percent

    if process.returncode == 0:
        print("Processing Complete.")
        return time.perf_counter() - start_time
    else:
        # Print error if FFmpeg fails to help debugging
        print(f"FFmpeg failed with return code {process.returncode}")
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
