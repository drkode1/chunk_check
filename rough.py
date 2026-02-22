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
                p = int((dl / total) * 100) if total > 0 else 0
                if p > last_p:
                    print(f"{p}% downloaded")
                    last_p = p
    return time.perf_counter() - start_time

def process_video(input_file):
    print("--- Phase 2: Processing ---")
    output_folder = "stream_output"
    os.makedirs(output_folder, exist_ok=True)
    playlist = os.path.abspath(os.path.join(output_folder, "index.m3u8"))
    
    start_time = time.perf_counter()
    
    # Minimalist command string
    # We use -v quiet to ensure NO logs are produced that could cause code 234
    ffmpeg_cmd = f'ffmpeg -y -i {input_file} -c copy -hls_time 10 -hls_list_size 0 -f hls "{playlist}" -v quiet'
    
    print("Executing FFmpeg...")
    # Using shell=True for maximum compatibility with the runner path
    result = subprocess.run(ffmpeg_cmd, shell=True)
    
    if result.returncode == 0:
        print("Processing Complete.")
        return time.perf_counter() - start_time
    else:
        print(f"FFmpeg failed. Code: {result.returncode}")
        return None

if __name__ == "__main__":
    url = os.getenv("DIRECT_DOWNLOAD_URL")
    file_name = "test_vid.mp4"
    
    if url:
        t_down = download_file(url, file_name)
        t_proc = process_video(file_name)
        
        if os.path.exists(file_name):
            os.remove(file_name)
            
        if t_proc:
            print(f"\n{t_down:.2f} {t_proc:.2f}")
        else:
            # If it fails, let's try to see if ffprobe can even read the file
            print("Running diagnostic on downloaded file...")
            subprocess.run(f"ffprobe -v error -show_format {file_name}", shell=True)
