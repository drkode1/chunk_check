import os
import time
import subprocess
import requests
import threading

def download_file(url, filename):
    print(f"--- Phase 1: Downloading File ---")
    start_download = time.perf_counter()
    
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0
    
    with open(filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024*1024):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    percent = (downloaded / total_size) * 100
                    print(f"\r{percent:.0f}% downloaded as of now", end="", flush=True)
    
    print("\nDownload Complete.")
    return time.perf_counter() - start_download

def process_to_hls(input_file):
    print(f"--- Phase 2: Processing to Chunks (FFmpeg) ---")
    output_folder = "stream_output"
    os.makedirs(output_folder, exist_ok=True)
    output_playlist = os.path.join(output_folder, "index.m3u8")
    
    start_proc = time.perf_counter()
    last_status_time = start_proc
    
    # FFmpeg command with -progress to track status
    cmd = [
        'ffmpeg', '-i', input_file,
        '-codec:', 'copy', 
        '-start_number', '0', 
        '-hls_time', '10', 
        '-hls_list_size', '0', 
        '-f', 'hls', output_playlist,
        '-y' # Overwrite if exists
    ]
    
    # Start the process
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    print("Video processing started...")

    while process.poll() is None:
        current_time = time.perf_counter()
        
        # Check if 10 minutes (600 seconds) have passed
        if current_time - last_status_time >= 600:
            elapsed_mins = int((current_time - start_proc) / 60)
            print(f"Still processing... {elapsed_mins} minutes elapsed.")
            last_status_time = current_time
        
        time.sleep(10) # Check status every 10 seconds

    if process.returncode == 0:
        print("Video processing complete.")
        return time.perf_counter() - start_proc
    else:
        print("\nError during FFmpeg processing.")
        return None

if __name__ == "__main__":
    file_url = os.getenv("DIRECT_DOWNLOAD_URL")
    temp_filename = "source_video.mp4"
    
    if not file_url:
        print("!! ERROR: DIRECT_DOWNLOAD_URL secret is missing !!")
    else:
        # Phase 1: Download
        time_download = download_file(file_url, temp_filename)
        
        # Phase 2: Process
        time_process = process_to_hls(temp_filename)
        
        # Cleanup
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

        # Final Clean Print
        print("\n" + "="*40)
        print(f"Download time: {time_download:.2f}s")
        if time_process:
            print(f"Video processing time: {time_process:.2f}s")
        print("="*40)
