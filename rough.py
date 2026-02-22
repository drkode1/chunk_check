import os
import time
import subprocess
import requests

def download_file(url, filename):
    print(f"--- Phase 1: Downloading File ---")
    start_time = time.perf_counter()
    
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
    return time.perf_counter() - start_time

def process_video(input_file):
    print(f"--- Phase 2: Video Processing ---")
    output_folder = "stream_output"
    os.makedirs(output_folder, exist_ok=True)
    
    start_time = time.perf_counter()
    last_print_time = start_time
    
    # FFmpeg: codec copy for speed, HLS for streaming chunks
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
    print("Processing started. Status updates every 10 minutes...")

    while True:
        retcode = process.poll()
        current_time = time.perf_counter()
        
        # 10-minute heartbeat (600 seconds)
        if current_time - last_print_time >= 600:
            elapsed_mins = int((current_time - start_time) / 60)
            print(f"Video processing status: Still running... ({elapsed_mins} minutes elapsed)")
            last_print_time = current_time
            
        if retcode is not None:
            break
        time.sleep(5)

    if retcode == 0:
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
            # Final output: {download_time} {process_time}
            print(f"\n{time_down:.2f} {time_proc:.2f}")
