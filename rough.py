import os
import time
import subprocess
import requests

def download_file(url, filename):
    print("--- Phase 1: Downloading ---")
    start_time = time.perf_counter()
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get('content-length', 0))
        dl = 0
        last_p = -1
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=16*1024*1024):
                f.write(chunk)
                dl += len(chunk)
                if total > 0:
                    p = int((dl / total) * 100)
                    if p > last_p:
                        print(f"{p}% downloaded")
                        last_p = p
    
    if os.path.exists(filename) and os.path.getsize(filename) > 0:
        print(f"Download Complete. Size: {os.path.getsize(filename) // (1024**2)} MB")
        return time.perf_counter() - start_time
    else:
        raise FileNotFoundError("Download failed.")

def process_video(input_file):
    print("--- Phase 2: Processing ---")
    output_folder = "stream_output"
    os.makedirs(output_folder, exist_ok=True)
    playlist = os.path.join(output_folder, "index.m3u8")
    
    start_time = time.perf_counter()
    
    # IMPROVED COMMAND:
    # -map 0:v:0 -> Only take the first video stream
    # -map 0:a:? -> Take all audio streams (if they exist)
    # -sn        -> Disable all subtitles (prevents WebVTT error)
    cmd = [
        'ffmpeg', '-y', '-i', input_file,
        '-map', '0:v:0', 
        '-map', '0:a:?', 
        '-c', 'copy',
        '-sn', 
        '-start_number', '0',
        '-hls_time', '10',
        '-hls_list_size', '0',
        '-f', 'hls', playlist,
        '-loglevel', 'error'
    ]
    
    print("Executing FFmpeg (Filtering streams)...")
    result = subprocess.run(cmd)
    
    if result.returncode == 0:
        print("Processing Complete.")
        return time.perf_counter() - start_time
    else:
        print(f"FFmpeg failed. Code: {result.returncode}")
        return None

if __name__ == "__main__":
    url = os.getenv("DIRECT_DOWNLOAD_URL")
    TARGET_FILE = "video_to_process.mp4"
    
    if url:
        try:
            t_down = download_file(url, TARGET_FILE)
            t_proc = process_video(TARGET_FILE)
            if os.path.exists(TARGET_FILE):
                os.remove(TARGET_FILE)
            if t_proc:
                print(f"\n{t_down:.2f} {t_proc:.2f}")
        except Exception as e:
            print(f"Error: {e}")
