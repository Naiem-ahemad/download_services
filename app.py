# downloader.py
import os
import threading
import time
import json
import re
import subprocess
import base64
import zlib
from fastapi import FastAPI, Query, HTTPException
import psutil
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import Config as BotocoreConfig
from dotenv import load_dotenv
from cryptography.fernet import Fernet

load_dotenv()

# ---------------- Settings ----------------
os.makedirs("./tmp", exist_ok=True)

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", b'wxk9V_lppKFwN1LzRroxrXOxKxhhRD2GhhxVhwLxflw=')
if isinstance(ENCRYPTION_KEY, str):
    ENCRYPTION_KEY = ENCRYPTION_KEY.encode("utf-8")
fernet = Fernet(ENCRYPTION_KEY)

app = FastAPI()
API_KEY = os.getenv("API_KEY") or "all-7f04e0d887372e3769b200d990ae7868"

# R2 Configuration
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_REGION = os.getenv("R2_REGION", "auto")
R2_STORAGE_LIMIT_GB = float(os.getenv("R2_STORAGE_LIMIT_GB", "9.5"))  # 9.5GB limit

# Only initialize R2 if credentials are available
if all([R2_ACCOUNT_ID, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET]):
    R2_ENDPOINT = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
    r2 = boto3.client(
        "s3",
        region_name=R2_REGION,
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        config=BotocoreConfig(signature_version="s3v4"),
    )
else:
    r2 = None
    print("Warning: R2 credentials not configured, upload will be disabled")

# In-memory task storage
tasks = {}
TASKS_FILE = "tasks.json"

def load_tasks():
    """Load tasks from file on startup"""
    global tasks
    if os.path.exists(TASKS_FILE):
        try:
            with open(TASKS_FILE, 'r') as f:
                tasks = json.load(f)
            print(f"Loaded {len(tasks)} tasks from {TASKS_FILE}")
        except Exception as e:
            print(f"Error loading tasks: {e}")
            tasks = {}

def save_tasks():
    """Save tasks to file"""
    try:
        with open(TASKS_FILE, 'w') as f:
            json.dump(tasks, f, indent=2)
    except Exception as e:
        print(f"Error saving tasks: {e}")

def decrypt_task_data(encrypted_task_id: str) -> dict:
    """Decrypt task data from encrypted task ID (matches first service)"""
    try:
        # base64-url-safe → decrypt → decompress → JSON
        encrypted = base64.urlsafe_b64decode(encrypted_task_id.encode("utf-8"))
        compressed = fernet.decrypt(encrypted)
        raw = zlib.decompress(compressed)
        return json.loads(raw.decode("utf-8"))
    except Exception as e:
        raise ValueError(f"Failed to decrypt task data: {e}")

def get_task(task_id):
    """Get task data - first try decryption, then fallback to memory storage"""
    try:
        # Try to decrypt the task_id to get the actual data
        return decrypt_task_data(task_id)
    except Exception:
        # Fallback: try to get from memory storage
        if task_id in tasks:
            return tasks[task_id]
        raise KeyError(f"Task {task_id} not found")

def set_task(task_id, data):
    """Set task data in memory storage using original task_id as key"""
    data["timestamp"] = time.time()
    tasks[task_id] = data
    # Save to file periodically
    if len(tasks) % 5 == 0:  # Save every 5 updates
        save_tasks()

def get_r2_storage_usage():
    """Get current R2 storage usage in GB"""
    if not r2:
        return 0.0
    
    try:
        response = r2.list_objects_v2(Bucket=R2_BUCKET)
        total_size = 0
        
        # Get size of all objects
        while True:
            if 'Contents' in response:
                for obj in response['Contents']:
                    total_size += obj['Size']
            
            # Handle pagination
            if response.get('IsTruncated'):
                response = r2.list_objects_v2(
                    Bucket=R2_BUCKET,
                    ContinuationToken=response['NextContinuationToken']
                )
            else:
                break
        
        return total_size / (1024 * 1024 * 1024)  # Convert to GB
    except Exception as e:
        print(f"Failed to get R2 storage usage: {e}")
        return 0.0

def is_r2_storage_full():
    """Check if R2 storage is near capacity"""
    usage_gb = get_r2_storage_usage()
    print(f"Current R2 usage: {usage_gb:.2f}GB / {R2_STORAGE_LIMIT_GB}GB")
    return usage_gb >= R2_STORAGE_LIMIT_GB

def bytes_to_mib_str(n):
    return f"{round(n / (1024*1024), 2)}MiB"

def parse_aria2_line(line):
    """Parse aria2c output line for progress info"""
    if not line:
        return None
    line = line.strip()
    
    patterns = [
        r"([\d\.]+[KkMmGgTtPp]i?[Bb]?)/([\d\.]+[KkMmGgTtPp]i?[Bb]?)\s*\((\d+)%\).*?DL:([\d\.]+\w*)",
        r"([\d\.]+[KkMmGgTtPp]i?[Bb]?)/([\d\.]+[KkMmGgTtPp]i?[Bb]?)\s*\((\d+)%\)",
        r"\((\d+)%\).*?DL:([\d\.]+\w*)",
        r"DL:([\d\.]+\w*)"
    ]
    
    for i, pattern in enumerate(patterns):
        match = re.search(pattern, line)
        if match:
            if i == 0:
                return {"downloaded": match.group(1), "total": match.group(2), 
                       "percent": int(match.group(3)), "speed": match.group(4)}
            elif i == 1:
                return {"downloaded": match.group(1), "total": match.group(2), 
                       "percent": int(match.group(3)), "speed": None}
            elif i == 2:
                return {"percent": int(match.group(1)), "speed": match.group(2)}
            elif i == 3:
                return {"speed": match.group(1)}
    return None

def run_with_progress(cmd, start, end, task_id, data, output_file=None, timeout=3600):
    """Run subprocess with progress tracking"""
    print(f"[{task_id}] Running command: {' '.join(cmd)}")
    
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
                              text=True, bufsize=1, universal_newlines=True)
    except FileNotFoundError:
        data["status"] = "error"
        data["error"] = f"Command not found: {cmd[0]} - Please install aria2c"
        set_task(task_id, data)
        print(f"[{task_id}] Error: {cmd[0]} not found")
        return False
    except Exception as e:
        data["status"] = "error"
        data["error"] = f"Failed to start process: {e}"
        set_task(task_id, data)
        print(f"[{task_id}] Error starting process: {e}")
        return False

    matched_any = False
    line_count = 0
    start_time = time.time()
    last_progress_time = start_time

    try:
        for raw_line in proc.stdout:
            current_time = time.time()
            
            # Check for timeout
            if current_time - start_time > timeout:
                proc.terminate()
                data["status"] = "error"
                data["error"] = f"Process timeout after {timeout} seconds"
                set_task(task_id, data)
                print(f"[{task_id}] Process timeout")
                return False
            
            line_count += 1
            line = raw_line.rstrip("\n")
            print(f"[{task_id}] aria2c: {line}")  # Debug output
            
            parsed = parse_aria2_line(line)

            if parsed:
                matched_any = True
                last_progress_time = current_time
                if parsed.get("percent") is not None:
                    percent = max(0, min(100, parsed["percent"]))
                    scaled = start + (percent * (end - start) // 100)
                    data["progress"] = scaled
                if parsed.get("speed") is not None:
                    data["speed"] = parsed["speed"]
                if parsed.get("downloaded") is not None:
                    data["downloaded"] = parsed["downloaded"]
                if parsed.get("total") is not None:
                    data["total"] = parsed["total"]

            # Check for stalled progress (no updates for 5 minutes)
            if current_time - last_progress_time > 300:
                proc.terminate()
                data["status"] = "error"
                data["error"] = "Process appears stalled - no progress updates"
                set_task(task_id, data)
                print(f"[{task_id}] Process stalled")
                return False

            # Set stage based on progress range
            if start == 0:
                data["stage"] = "video"
            elif start == 45:
                data["stage"] = "audio"

            # Update task data more frequently for better user feedback
            if line_count % 3 == 0 or parsed:
                set_task(task_id, data)
                
        # Wait for process to complete
        proc.wait(timeout=30)
            
    except Exception as e:
        try:
            proc.terminate()
            proc.wait()
        except:
            pass
        data["status"] = "error"
        data["error"] = f"Process exception: {e}"
        set_task(task_id, data)
        print(f"[{task_id}] Process exception: {e}")
        return False

    # Check return code
    if proc.returncode != 0:
        data["status"] = "error"
        data["error"] = f"Process failed with return code {proc.returncode}"
        set_task(task_id, data)
        print(f"[{task_id}] Process failed with return code {proc.returncode}")
        return False

    # Final progress update
    data["progress"] = end
    data["speed"] = "0MiB"
    if not matched_any and output_file and os.path.exists(output_file):
        size_bytes = os.path.getsize(output_file)
        data["downloaded"] = bytes_to_mib_str(size_bytes)
        data["total"] = data["downloaded"]

    set_task(task_id, data)
    print(f"[{task_id}] Process completed successfully")
    return True

class ProgressCallback:
    def __init__(self, task_id, data, start, end, total_bytes):
        self.task_id = task_id
        self.data = data
        self.start = start
        self.end = end
        self.total = float(total_bytes)
        self.lock = threading.Lock()
        self.uploaded = 0
        self.last_ts = time.time()

    def __call__(self, bytes_amount):
        with self.lock:
            now = time.time()
            self.uploaded += bytes_amount
            percent = min(100, int((self.uploaded / self.total) * 100)) if self.total > 0 else 0
            scaled = self.start + (percent * (self.end - self.start) // 100)

            dt = max(now - self.last_ts, 1e-6)
            speed_mib = (bytes_amount / dt) / (1024 * 1024)
            speed_str = f"{round(speed_mib, 2)}MiB"

            self.last_ts = now

            self.data["progress"] = min(scaled, self.end)
            self.data["stage"] = "uploading"
            self.data["uploaded"] = bytes_to_mib_str(self.uploaded)
            self.data["total"] = bytes_to_mib_str(int(self.total))
            self.data["speed"] = speed_str
            set_task(self.task_id, self.data)

def upload_with_progress(local_path, key, task_id, data, start=95, end=100, timeout=1800):
    """Upload file to R2 with progress tracking"""
    if not r2:
        raise RuntimeError("R2 client not configured")
        
    if not os.path.exists(local_path):
        raise FileNotFoundError(local_path)
    
    size_bytes = os.path.getsize(local_path)
    if size_bytes == 0:
        raise ValueError("File is empty")
        
    cb = ProgressCallback(task_id, data, start, end, size_bytes)
    transfer_config = TransferConfig(
        multipart_threshold=5 * 1024 * 1024,
        multipart_chunksize=5 * 1024 * 1024,
        max_concurrency=4,
        use_threads=True,
    )

    data["stage"] = "uploading"
    data["status"] = "uploading"
    data["progress"] = start
    data["uploaded"] = "0MiB"
    data["total"] = bytes_to_mib_str(size_bytes)
    data["speed"] = "0MiB"
    set_task(task_id, data)

    start_time = time.time()
    try:
        r2.upload_file(Filename=local_path, Bucket=R2_BUCKET, Key=key, Callback=cb, Config=transfer_config)
        
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Upload timeout after {timeout} seconds")
            
    except Exception as e:
        data["status"] = "error"
        data["error"] = f"Upload failed: {e}"
        set_task(task_id, data)
        raise

    data["progress"] = end
    data["uploaded"] = bytes_to_mib_str(size_bytes)
    data["speed"] = "0MiB"
    set_task(task_id, data)
    return True

def worker(task_id, original_task_data):
    """Main worker function for processing video tasks"""
    video_file = audio_file = output_file = None
    
    try:
        data = original_task_data.copy()
        
        # Validate required data
        if not data.get("video_format") or not data.get("audio_format"):
            raise ValueError("Missing video_format or audio_format in task data")
            
        video_url = data["video_format"].get("url")
        audio_url = data["audio_format"].get("url")
        
        if not video_url or not audio_url:
            raise ValueError("Missing video or audio URL")

        # Generate unique file paths - use a simpler ID for file names
        simple_id = str(abs(hash(task_id)))[:8]
        video_file = f"./tmp/{simple_id}_video.mp4"
        audio_file = f"./tmp/{simple_id}_audio.mp3"
        title = re.sub(r'[\\/*?:"<>|]', "_", data.get("title", "video"))[:50]
        ext = data["video_format"].get("ext", "mp4")
        output_file = f"./tmp/{title}_{simple_id}.{ext}"

        data["status"] = "downloading"
        data["stage"] = "video"
        set_task(task_id, data)
        
        print(f"[{task_id}] Starting video download from: {video_url}")
        video_success = run_with_progress([
            "aria2c", "-x", "16", "-s", "16", "-k", "1M",
            "--file-allocation=none",
            "--console-log-level=notice", "--summary-interval=1",
            "--timeout=60", "--retry-wait=3", "--max-tries=3",
            "-o", video_file, video_url
        ], 0, 45, task_id, data, output_file=video_file, timeout=1800)
        
        if not video_success:
            raise RuntimeError("Video download failed")
            
        if not os.path.exists(video_file) or os.path.getsize(video_file) == 0:
            raise RuntimeError("Video file is missing or empty after download")

        # Download audio
        print(f"[{task_id}] Starting audio download from: {audio_url}")
        data = tasks.get(task_id, data)  # Refresh data
        data["stage"] = "audio"
        set_task(task_id, data)
        
        audio_success = run_with_progress([
            "aria2c", 
            "-x", "8", "-s", "8", "-k", "1M",
            "--file-allocation=none", 
            "--console-log-level=info",
            "--timeout=60", 
            "--retry-wait=3", 
            "--max-tries=2",
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "-o", os.path.basename(audio_file),
            "-d", os.path.dirname(os.path.abspath(audio_file)),
            audio_url
        ], 45, 90, task_id, data, output_file=audio_file, timeout=1800)
        
        if not audio_success:
            raise RuntimeError("Audio download failed")
            
        if not os.path.exists(audio_file) or os.path.getsize(audio_file) == 0:
            raise RuntimeError("Audio file is missing or empty after download")

        # Merge video and audio
        print(f"[{task_id}] Starting merge")
        data = tasks.get(task_id, data)  # Refresh data
        data["stage"] = "merging"
        data["status"] = "merging"
        data["progress"] = 90
        set_task(task_id, data)

        try:
            result = subprocess.run([
                "ffmpeg", "-y", "-nostdin", 
                "-i", video_file, "-i", audio_file, 
                "-c", "copy", "-avoid_negative_ts", "make_zero",
                output_file
            ], capture_output=True, text=True, timeout=600)
            
            if result.returncode != 0:
                print(f"[{task_id}] FFmpeg stderr: {result.stderr}")
                raise RuntimeError(f"FFmpeg failed: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            raise RuntimeError("FFmpeg merge timeout after 10 minutes")
            
        if not os.path.exists(output_file) or os.path.getsize(output_file) == 0:
            raise RuntimeError("Output file is missing or empty after merge")

        data["progress"] = 95
        set_task(task_id, data)

        # Check R2 storage before uploading
        storage_full = is_r2_storage_full()
        
        if r2 and not storage_full:
            print(f"[{task_id}] Starting upload to R2")
            data = tasks.get(task_id, data)  # Refresh data
            r2_key = f"videos/{simple_id}.mp4"
            upload_with_progress(output_file, r2_key, task_id, data, 95, 100, timeout=1800)

            # Generate presigned URLs
            expire_seconds = 43200
            try:
                stream_url = r2.generate_presigned_url(
                    "get_object", 
                    Params={
                        "Bucket": R2_BUCKET, 
                        "Key": r2_key, 
                        "ResponseContentType": "video/mp4", 
                        "ResponseContentDisposition": "inline"
                    }, 
                    ExpiresIn=expire_seconds
                )
                download_url = r2.generate_presigned_url(
                    "get_object", 
                    Params={
                        "Bucket": R2_BUCKET, 
                        "Key": r2_key, 
                        "ResponseContentType": "video/mp4", 
                        "ResponseContentDisposition": f'attachment; filename="{os.path.basename(output_file)}"'
                    }, 
                    ExpiresIn=expire_seconds
                )
            except Exception as e:
                raise RuntimeError(f"Failed to generate URLs: {e}")
        else:
            # R2 storage full or not configured - serve files directly
            reason = "R2 storage full" if storage_full else "R2 not configured"
            print(f"[{task_id}] {reason}, serving files directly")
            
            # Create direct streaming URLs to our server
            encoded_path = base64.urlsafe_b64encode(output_file.encode()).decode()
            stream_url = f"/stream/{encoded_path}?key={API_KEY}"
            download_url = f"/download-file/{encoded_path}?key={API_KEY}&filename={os.path.basename(output_file)}"

        # Mark as completed
        data["status"] = "done"
        data["stage"] = "finished"
        data["progress"] = 100
        data["stream_url"] = stream_url
        data["download_url"] = download_url
        data["local_file"] = output_file
        data["storage_mode"] = "r2" if (r2 and not storage_full) else "direct"
        set_task(task_id, data)
        
        print(f"[{task_id}] Task completed successfully")

    except Exception as e:
        print(f"[{task_id}] Task failed: {str(e)}")
        try:
            data = tasks.get(task_id, {"timestamp": time.time()})
        except:
            data = {"timestamp": time.time()}
        
        data["status"] = "error"
        data["error"] = str(e)
        set_task(task_id, data)
        
    finally:
        # Cleanup temporary files - but keep output file if serving directly
        storage_mode = data.get("storage_mode", "direct")
        files_to_cleanup = [video_file, audio_file]
        
        # Only cleanup output file if we uploaded to R2
        if storage_mode == "r2":
            files_to_cleanup.append(output_file)
            
        for file_path in files_to_cleanup:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    print(f"[{task_id}] Cleaned up {file_path}")
                except Exception as e:
                    print(f"[{task_id}] Failed to cleanup {file_path}: {e}")

# Concurrency control
MAX_PARALLEL_TASKS = int(os.getenv("MAX_PARALLEL_TASKS", 3))
task_semaphore = threading.Semaphore(MAX_PARALLEL_TASKS)
active_workers = {}

def start_task_in_background(task_id, task_data):
    """Start task processing in background thread"""
    def run_worker():
        try:
            print(f"[{task_id}] Worker thread started")
            active_workers[task_id] = threading.current_thread()
            worker(task_id, task_data)
        except Exception as e:
            print(f"[{task_id}] Worker thread exception: {e}")
            data = tasks.get(task_id, {"timestamp": time.time()})
            data["status"] = "error"
            data["error"] = f"Worker thread exception: {str(e)}"
            set_task(task_id, data)
        finally:
            active_workers.pop(task_id, None)
            task_semaphore.release()
            print(f"[{task_id}] Worker thread finished")

    # Check if already running
    if task_id in active_workers:
        print(f"[{task_id}] Task already running")
        return

    # Try to acquire semaphore
    if not task_semaphore.acquire(blocking=False):
        print(f"[{task_id}] No available slots, marking as queued")
        data = tasks.get(task_id, {"timestamp": time.time()})
        data["status"] = "queued"
        set_task(task_id, data)
        return

    # Initialize task data and start worker
    data = tasks.get(task_id, {"timestamp": time.time()})
    data.update({
        "status": "running",
        "stage": "video",
        "progress": 0,
        "downloaded": "0MiB",
        "total": None,
        "speed": "0MiB"
    })
    set_task(task_id, data)

    thread = threading.Thread(target=run_worker, daemon=True)
    thread.start()
    print(f"[{task_id}] Background worker started")

# FastAPI endpoints
@app.get("/progress/{task_id}")
def get_progress(task_id: str, key: str = Query(...)):
    """Get task progress - automatically start task if needed"""
    if key != API_KEY:
        raise HTTPException(403, "Invalid API key")
    
    try:
        # Try to get decrypted task data first
        original_task_data = decrypt_task_data(task_id)
        
        # Check if we have this task in our memory storage
        if task_id not in tasks:
            # This is a new task, initialize it
            print(f"[{task_id}] New task detected, initializing")
            initial_data = {
                "status": "waiting",
                "stage": None,
                "progress": 0,
                "timestamp": time.time()
            }
            set_task(task_id, initial_data)
        
        # Get current task data from memory
        data = tasks[task_id]
        
        # Auto-start if waiting
        if data.get("status") == "waiting":
            print(f"[{task_id}] Task waiting, attempting to start")
            start_task_in_background(task_id, original_task_data)
            # Refresh data after starting
            data = tasks.get(task_id, data)

        return {
            "status": data.get("status"),
            "stage": data.get("stage"),
            "progress": data.get("progress", 0),
            "downloaded": data.get("downloaded"),
            "total": data.get("total"),
            "speed": data.get("speed"),
            "stream_url": data.get("stream_url"),
            "download_url": data.get("download_url"),
            "error": data.get("error"),
            "local_file": data.get("local_file"),
        }
        
    except ValueError as e:
        # Decryption failed
        raise HTTPException(400, f"Invalid task ID: {str(e)}")
    except Exception as e:
        # Other errors
        raise HTTPException(500, f"Internal error: {str(e)}")

@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": time.time()}

@app.get("/stats")
def get_stats(key: str = Query(...)):
    """Get system and task statistics"""
    if key != API_KEY:
        raise HTTPException(403, "Invalid API key")

    total_tasks = len(tasks)
    active_tasks = len([t for t in tasks.values() if t.get("status") in ("running", "uploading", "merging", "downloading")])
    queued_tasks = len([t for t in tasks.values() if t.get("status") == "queued"])
    error_tasks = len([t for t in tasks.values() if t.get("status") == "error"])
    done_tasks = len([t for t in tasks.values() if t.get("status") == "done"])

    try:
        mem = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=0.1)
        memory_usage = f"{round(mem.used / (1024*1024), 2)} MiB / {round(mem.total / (1024*1024), 2)} MiB"
    except:
        memory_usage = "N/A"
        cpu = 0

    return {
        "total_tasks": total_tasks,
        "active_tasks": active_tasks,
        "queued_tasks": queued_tasks,
        "error_tasks": error_tasks,
        "done_tasks": done_tasks,
        "active_workers": len(active_workers),
        "semaphore_available": task_semaphore._value,
        "memory_usage": memory_usage,
        "cpu_usage_percent": cpu,
        "r2_configured": r2 is not None,
        "r2_storage_usage_gb": get_r2_storage_usage() if r2 else 0,
        "r2_storage_limit_gb": R2_STORAGE_LIMIT_GB,
        "r2_storage_full": is_r2_storage_full() if r2 else False
    }

# Direct file streaming endpoints
@app.get("/stream/{encoded_path}")
async def stream_video(encoded_path: str, key: str = Query(...)):
    """Stream video file directly from server with range support"""
    from fastapi.responses import FileResponse
    from fastapi import Request
    
    if key != API_KEY:
        raise HTTPException(403, "Invalid API key")
    
    try:
        file_path = base64.urlsafe_b64decode(encoded_path.encode()).decode()
    except Exception:
        raise HTTPException(400, "Invalid file path")
    
    if not os.path.exists(file_path):
        raise HTTPException(404, "File not found")
    
    # Return file with proper headers for video streaming
    return FileResponse(
        path=file_path,
        media_type="video/mp4",
        headers={
            "Accept-Ranges": "bytes",
            "Content-Disposition": "inline"
        }
    )

@app.get("/download-file/{encoded_path}")
async def download_file(encoded_path: str, filename: str = Query("video.mp4"), key: str = Query(...)):
    """Download file directly from server"""
    from fastapi.responses import FileResponse
    import urllib.parse
    
    if key != API_KEY:
        raise HTTPException(403, "Invalid API key")
    
    try:
        file_path = base64.urlsafe_b64decode(encoded_path.encode()).decode()
    except Exception:
        raise HTTPException(400, "Invalid file path")
    
    if not os.path.exists(file_path):
        raise HTTPException(404, "File not found")
    
    # Encode filename for proper download
    encoded_filename = urllib.parse.quote(filename)
    
    return FileResponse(
        path=file_path,
        media_type="video/mp4",
        filename=filename,
        headers={
            "Content-Disposition": f'attachment; filename*=UTF-8\'\'{encoded_filename}'
        }
    )

# Cleanup function
def cleanup_old_tasks():
    """Cleanup old tasks and files"""
    while True:
        try:
            current_time = time.time()
            old_tasks = []
            for task_id, data in list(tasks.items()):
                # For direct storage mode, keep files longer (24 hours vs 12 hours)
                storage_mode = data.get("storage_mode", "direct")
                max_age = 86400 if storage_mode == "direct" else 43200  # 24h vs 12h
                
                if current_time - data.get("timestamp", 0) > max_age:
                    # Clean up R2 files
                    if storage_mode == "r2" and data.get("status") == "done":
                        try:
                            simple_id = str(abs(hash(task_id)))[:8]
                            r2.delete_object(Bucket=R2_BUCKET, Key=f"videos/{simple_id}.mp4")
                        except:
                            pass
                    
                    # Clean up local files (for direct storage mode)
                    local_file = data.get("local_file")
                    if local_file and os.path.exists(local_file):
                        try:
                            os.remove(local_file)
                            print(f"Cleaned up local file: {local_file}")
                        except:
                            pass
                            
                    old_tasks.append(task_id)

            for tid in old_tasks:
                tasks.pop(tid, None)
                active_workers.pop(tid, None)
                
            if old_tasks:
                save_tasks()
                print(f"Cleaned up {len(old_tasks)} old tasks")
                
        except Exception as e:
            print(f"Cleanup error: {e}")
        time.sleep(300)  # Every 5 minutes

# Load tasks on startup and start cleanup
load_tasks()
threading.Thread(target=cleanup_old_tasks, daemon=True).start()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)