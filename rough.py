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
    
    # Verify file exists and has size
    if os.path.exists(filename) and os.path.getsize(filename) > 0:
        print(f"Download Complete. Size: {os.path.getsize(filename) // (1024**2)} MB")
        return time.perf_counter() - start_time
    else:
        raise FileNotFoundError("Download failed: File not found on disk.")

def process_video(input_file):
    print("--- Phase 2: Processing ---")
    if not os.path.exists(input_file):
        print(f"Error: {input_file} missing before FFmpeg start.")
        return None

    output_folder = "stream_output"
    os.makedirs(output_folder, exist_ok=True)
    playlist = os.path.join(output_folder, "index.m3u8")
    
    start_time = time.perf_counter()
    
    # Using a list instead of a string for subprocess.run is safer in Linux
    cmd = [
        'ffmpeg', '-y', '-i', input_file,
        '-c', 'copy',
        '-hls_time', '10',
        '-hls_list_size', '0',
        '-f', 'hls', playlist,
        '-loglevel', 'error'
    ]
    
    print("Executing FFmpeg...")
    # No piping to avoid buffer overflow (Error 234)
    result = subprocess.run(cmd)
    
    if result.returncode == 0:
        print("Processing Complete.")
        return time.perf_counter() - start_time
    else:
        print(f"FFmpeg failed. Code: {result.returncode}")
        return None

if __name__ == "__main__":
    url = os.getenv("DIRECT_DOWNLOAD_URL")
    # THE FIX: Ensure both phases use the EXACT same filename
    TARGET_FILE = "video_to_process.mp4"
    
    if not url:
        print("Error: DIRECT_DOWNLOAD_URL not found.")
    else:
        try:
            t_down = download_file(url, TARGET_FILE)
            t_proc = process_video(TARGET_FILE)
            
            # Clean up
            if os.path.exists(TARGET_FILE):
                os.remove(TARGET_FILE)
                
            if t_proc:
                # Final requirement: Print {down_time} {proc_time}
                print(f"\n{t_down:.2f} {t_proc:.2f}")
        except Exception as e:
            print(f"Pipeline failed: {e}")
