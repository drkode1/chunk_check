import os
import time
import subprocess
import requests

def download_file(url, filename):
    print(f"--- Phase 1: Downloading File ---")
    start_download = time.perf_counter()
    
    r = requests.get(url, stream=True)
    r.raise_for_status()
    
    total_size = int(r.headers.get('content-length', 0))
    downloaded = 0
    
    with open(filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024*1024):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
    
    end_download = time.perf_counter()
    return end_download - start_download

def process_to_hls(input_file):
    print(f"--- Phase 2: Processing to Chunks (FFmpeg) ---")
    output_folder = "stream_output"
    os.makedirs(output_folder, exist_ok=True)
    output_playlist = os.path.join(output_folder, "index.m3u8")
    
    start_proc = time.perf_counter()
    
    # Use -codec: copy for "Exact Original" quality at high speed
    cmd = [
        'ffmpeg', '-i', input_file,
        '-codec:', 'copy', 
        '-start_number', '0', 
        '-hls_time', '10', 
        '-hls_list_size', '0', 
        '-f', 'hls', output_playlist
    ]
    
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        end_proc = time.perf_counter()
        return end_proc - start_proc
    except subprocess.CalledProcessError as e:
        print(f"Error during FFmpeg: {e}")
        return None

if __name__ == "__main__":
    file_url = os.getenv("DIRECT_DOWNLOAD_URL")
    temp_filename = "source_video.mp4"
    
    if not file_url:
        print("!! ERROR: DIRECT_DOWNLOAD_URL secret is missing !!")
    else:
        # 1. Download & Time it
        time_download = download_file(file_url, temp_filename)
        
        # 2. Process & Time it
        time_process = process_to_hls(temp_filename)
        
        # 3. Cleanup
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

        # 4. Final Report
        print("\n" + "="*30)
        print("WORKFLOW TIME REPORT")
        print("="*30)
        print(f"1. Download Time:   {time_download:.2f} seconds")
        if time_process:
            print(f"2. Processing Time: {time_process:.2f} seconds")
            print(f"TOTAL TIME:         {time_download + time_process:.2f} seconds")
        else:
            print("2. Processing:      FAILED")
        print("="*30)
