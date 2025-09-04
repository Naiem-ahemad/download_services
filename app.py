# app_fixed.py
import os
import threading
import time
import json
import re
import subprocess
from typing import Dict, Any
from fastapi import FastAPI, Query, HTTPException
import psutil
import boto3
import redis
from boto3.s3.transfer import TransferConfig
from botocore.client import Config as BotocoreConfig
from dotenv import load_dotenv
from cryptography.fernet import Fernet

load_dotenv()

# ---------------- Settings ----------------
TEMP_DIR = "./videos"
os.makedirs(TEMP_DIR, exist_ok=True)

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
if not ENCRYPTION_KEY:
    raise RuntimeError("ENCRYPTION_KEY missing in environment")
# Fernet needs bytes
if isinstance(ENCRYPTION_KEY, str):
    ENCRYPTION_KEY = ENCRYPTION_KEY.encode("utf-8")
fernet = Fernet(ENCRYPTION_KEY)

app = FastAPI()
API_KEY = os.getenv("API_KEY") or "all-7f04e0d887372e3769b200d990ae7868"

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_REGION = os.getenv("R2_REGION", "auto")
R2_ENDPOINT = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com" if R2_ACCOUNT_ID else None

# boto3 client (guard if missing)
r2 = boto3.client(
    "s3",
    region_name=(R2_REGION if R2_REGION else None),
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=BotocoreConfig(signature_version="s3v4"),
)

redis_client = redis.Redis(
    host="redis-16144.c279.us-central1-1.gce.redns.redis-cloud.com",
    port=16144,
    username="default",
    password="ePDaUFP9aJmPfYiLAqnqGk6MD6RmarKp",  # your Redis password
    decode_responses=True,  # ensures strings instead of bytes
)

@app.get("/debug/tasks")
def debug_tasks(key: str = Query(...)):
    if key != API_KEY:
        raise HTTPException(403, "Invalid API key")

    all_tasks = []
    for task_id, task in tasks.items():
        all_tasks.append({
            "task_id": task_id,
            "raw": task
        })

    return {"count": len(all_tasks), "tasks": all_tasks}

# small helper wrapper around redis to store {"encrypted": "..."}
class RedisDict:
    def __init__(self, redis_client, prefix="task:"):
        self.r = redis_client
        self.prefix = prefix

    def _key(self, k):
        return f"{self.prefix}{k}"

    def __setitem__(self, k, v):
        self.r.set(self._key(k), json.dumps(v))

    def __getitem__(self, k):
        raw = self.r.get(self._key(k))
        if raw is None:
            raise KeyError(k)
        return json.loads(raw)

    def __delitem__(self, k):
        self.r.delete(self._key(k))

    def __contains__(self, k):
        return self.r.exists(self._key(k)) > 0

    def items(self):
        keys = self.r.keys(f"{self.prefix}*") or []
        for key in keys:
            if isinstance(key, bytes):
                key = key.decode()
            task_id = key.replace(self.prefix, "")
            raw = self.r.get(self._key(task_id))
            if raw:
                yield task_id, json.loads(raw)

    def pop(self, k, default=None):
        try:
            val = self.__getitem__(k)
            self.__delitem__(k)
            return val
        except KeyError:
            return default

    def keys(self):
        keys = self.r.keys(f"{self.prefix}*") or []
        return [(k.decode() if isinstance(k, bytes) else k).replace(self.prefix, "") for k in keys]

    def __len__(self):
        return len(self.keys())


tasks = RedisDict(redis_client)

# helper to write encrypted task state (always include timestamp)
def set_task(task_id: str, data: dict) -> None:
    data = dict(data)  # copy to avoid shared-mutation surprises
    data["timestamp"] = time.time()
    encrypted = fernet.encrypt(json.dumps(data, ensure_ascii=False).encode("utf-8")).decode("utf-8")
    tasks[task_id] = {"encrypted": encrypted}

def get_task(task_id: str) -> dict:
    raw = tasks[task_id]  # may raise KeyError
    enc = raw["encrypted"]
    dec = fernet.decrypt(enc.encode("utf-8"))
    return json.loads(dec.decode("utf-8"))


# utility
def bytes_to_mib_str(n):
    return f"{round(n / (1024*1024), 2)}MiB"


# parse aria2 lines (unchanged but safe-guarded)
def parse_aria2_line(line: str):
    if not line:
        return None
    line = line.strip()
    p1 = re.search(r"([\d\.]+[KkMmGgTtPp]i?[Bb]?)/([\d\.]+[KkMmGgTtPp]i?[Bb]?)\s*\((\d+)%\).*?DL:([\d\.]+\w*)", line)
    if p1:
        return {"downloaded": p1.group(1), "total": p1.group(2), "percent": int(p1.group(3)), "speed": p1.group(4)}
    p2 = re.search(r"([\d\.]+[KkMmGgTtPp]i?[Bb]?)/([\d\.]+[KkMmGgTtPp]i?[Bb]?)\s*\((\d+)%\)", line)
    if p2:
        return {"downloaded": p2.group(1), "total": p2.group(2), "percent": int(p2.group(3)), "speed": None}
    p3 = re.search(r"\((\d+)%\).*?DL:([\d\.]+\w*)", line)
    if p3:
        return {"percent": int(p3.group(1)), "speed": p3.group(2)}
    p4 = re.search(r"DL:([\d\.]+\w*)", line)
    if p4:
        return {"speed": p4.group(1)}
    return None


def run_with_progress(cmd, start, end, task_id, data, output_file=None, update_every_n_lines=1, timeout=3600):
    """Run subprocess and stream parse stdout to update task progress with timeout."""
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
    except FileNotFoundError as e:
        data["status"] = "error"
        data["error"] = f"command not found: {cmd[0]}"
        set_task(task_id, data)
        return False
    except Exception as e:
        data["status"] = "error"
        data["error"] = f"failed to start process: {e}"
        set_task(task_id, data)
        return False

    matched_any = False
    line_count = 0
    start_time = time.time()
    last_progress_time = start_time

    try:
        # iterate lines as they appear
        for raw_line in proc.stdout:
            current_time = time.time()
            
            # Check for timeout
            if current_time - start_time > timeout:
                proc.kill()
                data["status"] = "error"
                data["error"] = f"Process timeout after {timeout} seconds"
                set_task(task_id, data)
                return False
            
            line_count += 1
            line = raw_line.rstrip("\n")
            parsed = parse_aria2_line(line)

            if parsed:
                matched_any = True
                last_progress_time = current_time
                if parsed.get("percent") is not None:
                    percent = parsed["percent"]
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
                proc.kill()
                data["status"] = "error"
                data["error"] = "Process appears stalled - no progress updates"
                set_task(task_id, data)
                return False

            # always keep stage set
            if start == 0:
                data["stage"] = "video"
            elif start == 45:
                data["stage"] = "audio"

            # ✅ force update even if not parsed
            if (line_count % update_every_n_lines == 0) or parsed:
                set_task(task_id, data)
                
        # after loop finished, wait for process to exit with timeout
        try:
            proc.wait(timeout=30)  # Wait max 30 seconds for clean exit
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            
    except Exception as e:
        try:
            proc.kill()
            proc.wait()
        except Exception:
            pass
        data["status"] = "error"
        data["error"] = f"run_with_progress exception: {e}"
        set_task(task_id, data)
        return False

    # Check if process completed successfully
    if proc.returncode != 0:
        data["status"] = "error"
        data["error"] = f"Process failed with return code {proc.returncode}"
        set_task(task_id, data)
        return False

    # Completed run, ensure final progress & fallback
    data["progress"] = end
    data["speed"] = "0MiB"
    if not matched_any and output_file and os.path.exists(output_file):
        size_bytes = os.path.getsize(output_file)
        data["downloaded"] = bytes_to_mib_str(size_bytes)
        data["total"] = data["downloaded"]

    set_task(task_id, data)
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
        self.last_uploaded = 0
        self.recent_bytes = 0
        self.recent_ts = self.last_ts

    def __call__(self, bytes_amount):
        with self.lock:
            now = time.time()
            self.uploaded += bytes_amount
            percent = int((self.uploaded / self.total) * 100) if self.total > 0 else 0
            percent = min(percent, 100)
            scaled = self.start + (percent * (self.end - self.start) // 100)

            dt = now - self.recent_ts
            if dt <= 0:
                speed_str = "0MiB"
            else:
                self.recent_bytes += bytes_amount
                if dt >= 0.5:
                    speed_bps = self.recent_bytes / dt
                    speed_mib = speed_bps / (1024 * 1024)
                    speed_str = f"{round(speed_mib, 2)}MiB"
                    self.recent_ts = now
                    self.recent_bytes = 0
                else:
                    delta = self.uploaded - self.last_uploaded
                    dt_full = now - self.last_ts if (now - self.last_ts) > 0 else 1e-6
                    speed_mib = (delta / dt_full) / (1024 * 1024)
                    speed_str = f"{round(speed_mib, 2)}MiB"

            self.last_uploaded = self.uploaded
            self.last_ts = now
            uploaded_hr = bytes_to_mib_str(self.uploaded)
            total_hr = bytes_to_mib_str(self.total)

            self.data["progress"] = min(scaled, self.end)
            self.data["stage"] = "uploading"
            self.data["uploaded"] = uploaded_hr
            self.data["total"] = total_hr
            self.data["speed"] = speed_str
            set_task(self.task_id, self.data)


def upload_with_progress(local_path, key, task_id, data, start=95, end=100, timeout=1800):
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
        
        # Check if upload took too long
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Upload timeout after {timeout} seconds")
            
    except Exception as e:
        data["status"] = "error"
        data["error"] = f"upload failed: {e}"
        set_task(task_id, data)
        raise

    data["progress"] = end
    data["uploaded"] = bytes_to_mib_str(size_bytes)
    data["speed"] = "0MiB"
    set_task(task_id, data)
    return True


def worker(task_id: str):
    video_file = audio_file = output_file = None
    
    try:
        data = get_task(task_id)
        
        # Validate required data
        if not data.get("video_format") or not data.get("audio_format"):
            raise ValueError("Missing video_format or audio_format in task data")
            
        video_url = data["video_format"]["url"]
        audio_url = data["audio_format"]["url"]
        
        if not video_url or not audio_url:
            raise ValueError("Missing video or audio URL")

        os.makedirs("./tmp", exist_ok=True)
        video_file = f"./tmp/{task_id}_video.mp4"
        audio_file = f"./tmp/{task_id}_audio.mp3"
        title = re.sub(r'[\\/*?:"<>|]', "_", data.get("title", "video"))
        ext = data["video_format"].get("ext", "mp4")
        output_file = f"./tmp/{title}.{ext}"

        # Video download with improved error handling
        print(f"[{task_id}] Starting video download")
        data["status"] = "downloading"
        data["stage"] = "video"
        set_task(task_id, data)
        
        ok = run_with_progress(
            ["aria2c", "-x", "16", "-s", "16", "-k", "1M",
                "--file-allocation=none",
                "--console-log-level=notice", "--summary-interval=1",
                "--timeout=60", "--retry-wait=3", "--max-tries=3",
                "-o", video_file, video_url],
            0, 45, task_id, data, output_file=video_file, timeout=1800
        )
        if not ok:
            raise RuntimeError("Video download failed")
            
        # Verify video file exists and has content
        if not os.path.exists(video_file) or os.path.getsize(video_file) == 0:
            raise RuntimeError("Video file is missing or empty after download")

        # Audio download
        print(f"[{task_id}] Starting audio download")
        data = get_task(task_id)
        data["stage"] = "audio"
        set_task(task_id, data)
        
        ok = run_with_progress(
            ["aria2c", "-x", "16", "-s", "16", "-k", "1M", 
             "--file-allocation=none", "--timeout=60", "--retry-wait=3", "--max-tries=3",
             "-o", audio_file, audio_url],
            45, 90, task_id, data, output_file=audio_file, timeout=1800
        )
        if not ok:
            raise RuntimeError("Audio download failed")
            
        # Verify audio file exists and has content
        if not os.path.exists(audio_file) or os.path.getsize(audio_file) == 0:
            raise RuntimeError("Audio file is missing or empty after download")

        # Merge with timeout
        print(f"[{task_id}] Starting merge")
        data = get_task(task_id)
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
            ], capture_output=True, text=True, timeout=600)  # 10 minute timeout
            
            if result.returncode != 0:
                raise RuntimeError(f"FFmpeg failed: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            raise RuntimeError("FFmpeg merge timeout after 10 minutes")
            
        # Verify output file
        if not os.path.exists(output_file) or os.path.getsize(output_file) == 0:
            raise RuntimeError("Output file is missing or empty after merge")

        data["progress"] = 95
        set_task(task_id, data)

        # Upload with improved error handling
        print(f"[{task_id}] Starting upload")
        data = get_task(task_id)
        data["stage"] = "uploading"
        data["status"] = "uploading"
        set_task(task_id, data)

        r2_key = f"videos/{task_id}.mp4"
        upload_with_progress(output_file, r2_key, task_id, data, 95, 100, timeout=1800)

        # Generate URLs
        expire_seconds = 43200
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

        # Final success update
        data["status"] = "done"
        data["stage"] = "finished"
        data["progress"] = 100
        data["stream_url"] = stream_url
        data["download_url"] = download_url
        set_task(task_id, data)
        
        print(f"[{task_id}] Task completed successfully")

    except Exception as e:
        print(f"[{task_id}] Task failed with error: {str(e)}")
        try:
            data = get_task(task_id)
        except:
            data = {}
        data["status"] = "error"
        data["error"] = str(e)
        set_task(task_id, data)
    finally:
        # Cleanup files
        for f in (video_file, audio_file, output_file):
            try:
                if f and os.path.exists(f):
                    os.remove(f)
                    print(f"[{task_id}] Cleaned up {f}")
            except Exception as e:
                print(f"[{task_id}] Failed to cleanup {f}: {e}")


# concurrency control
MAX_PARALLEL_TASKS = int(os.getenv("MAX_PARALLEL_TASKS", 4))
task_semaphore = threading.Semaphore(MAX_PARALLEL_TASKS)
active_workers = {}  # Track active worker threads


def start_task_in_background(task_id: str):
    """Launch worker thread which reads fresh task data from Redis."""
    def run_worker(task_id):
        try:
            print(f"[{task_id}] Worker thread started")
            active_workers[task_id] = threading.current_thread()
            worker(task_id)
        except Exception as e:
            print(f"[{task_id}] Worker thread exception: {e}")
            try:
                data = get_task(task_id)
            except:
                data = {}
            data["status"] = "error"
            data["error"] = f"Worker thread exception: {str(e)}"
            set_task(task_id, data)
        finally:
            try:
                active_workers.pop(task_id, None)
                task_semaphore.release()
                print(f"[{task_id}] Worker thread finished, semaphore released")
            except Exception as e:
                print(f"[{task_id}] Error in worker cleanup: {e}")

    # Check if task is already running
    if task_id in active_workers:
        print(f"[{task_id}] Task already running")
        return

    acquired = task_semaphore.acquire(blocking=False)
    if not acquired:
        print(f"[{task_id}] No available slots, marking as queued")
        # mark queued
        try:
            data = get_task(task_id)
        except KeyError:
            data = {}
        data["status"] = "queued"
        set_task(task_id, data)
        return

    # set initial values and start
    try:
        data = get_task(task_id)
    except KeyError:
        data = {}
    data.setdefault("status", "running")
    data.setdefault("stage", "video")
    data.setdefault("progress", 0)
    data.setdefault("downloaded", "0MiB")
    data.setdefault("total", None)
    data.setdefault("speed", "0MiB")
    set_task(task_id, data)

    t = threading.Thread(target=run_worker, args=(task_id,), daemon=True)
    t.start()
    print(f"[{task_id}] Background worker thread started")


# endpoints
@app.get("/progress/{task_id}")
def progress(task_id: str, key: str = Query(...)):
    if key != API_KEY:
        raise HTTPException(403, "Invalid API key")
    if task_id not in tasks:
        raise HTTPException(404, "Task not found")

    task = tasks[task_id]
    data = json.loads(fernet.decrypt(task["encrypted"].encode()).decode())
    
    # if waiting -> try to start
    if data.get("status") == "waiting":
        print(f"[{task_id}] Task in waiting status, attempting to start")
        start_task_in_background(task_id)
        # re-read latest
        try:
            task = tasks[task_id]
            data = json.loads(fernet.decrypt(task["encrypted"].encode()).decode())
        except:
            pass

    return {
        "status": data.get("status"),
        "stage": data.get("stage"),
        "progress": data.get("progress", 0),
        "downloaded": data.get("downloaded"),
        "total": data.get("total"),
        "speed": data.get("speed"),
        "stream_url": data.get("stream_url"),
        "download_url": data.get("download_url"),
        "error": data.get("error"),  # Include error in response
    }


@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": time.time()}


def cleanup_old_tasks():
    while True:
        try:
            current_time = time.time()
            old_tasks = []
            for task_id, task in list(tasks.items()):
                try:
                    data = json.loads(fernet.decrypt(task["encrypted"].encode()).decode())
                    # Clean up tasks older than 12 hours
                    if current_time - data.get("timestamp", 0) > 43200:
                        key = f"videos/{task_id}.mp4"
                        try:
                            r2.delete_object(Bucket=R2_BUCKET, Key=key)
                            print(f"Deleted old R2 object: {key}")
                        except Exception as e:
                            print(f"Failed to delete R2 object {key}: {e}")
                        old_tasks.append(task_id)
                except Exception as e:
                    print(f"Error processing task {task_id} for cleanup: {e}")
                    old_tasks.append(task_id)

            for tid in old_tasks:
                try:
                    tasks.pop(tid, None)
                    active_workers.pop(tid, None)  # Clean up worker tracking
                    print(f"Cleaned up old task: {tid}")
                except Exception as e:
                    print(f"Error cleaning up task {tid}: {e}")
                    
        except Exception as e:
            print("Cleanup error:", e)
        time.sleep(300)  # Run every 5 minutes


@app.get("/stats")
def stats(key: str = Query(...)):
    if key != API_KEY:
        raise HTTPException(403, "Invalid API key")

    total_tasks = len(tasks)
    active_tasks = 0
    queued_tasks = 0
    error_tasks = 0
    total_downloaded_bytes = 0
    task_details = []

    for task_id, task in tasks.items():
        data = json.loads(fernet.decrypt(task["encrypted"].encode()).decode())
        status = data.get("status")
        
        if status in ("running", "uploading", "merging", "downloading"):
            active_tasks += 1
        elif status == "queued":
            queued_tasks += 1
        elif status == "error":
            error_tasks += 1

        downloaded_str = data.get("downloaded")
        if downloaded_str:
            match = re.match(r"([\d\.]+)([KMG]i?)B", downloaded_str)
            if match:
                num, unit = match.groups()
                num = float(num)
                factor = {"K": 1024, "Ki": 1024, "M": 1024**2, "Mi": 1024**2, "G": 1024**3, "Gi": 1024**3}.get(unit, 1)
                total_downloaded_bytes += int(num * factor)

        task_details.append({
            "task_id": task_id,
            "status": status,
            "stage": data.get("stage"),
            "progress": data.get("progress", 0),
            "downloaded": data.get("downloaded"),
            "total": data.get("total"),
            "speed": data.get("speed"),
            "error": data.get("error"),
            "timestamp": data.get("timestamp"),
        })

    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=0.1)

    return {
        "total_tasks": total_tasks,
        "active_tasks": active_tasks,
        "queued_tasks": queued_tasks,
        "error_tasks": error_tasks,
        "active_workers": len(active_workers),
        "semaphore_available": task_semaphore._value,
        "total_downloaded": f"{round(total_downloaded_bytes / (1024*1024), 2)} MiB",
        "tasks": task_details,
        "memory_usage": f"{round(mem.used / (1024*1024), 2)} MiB / {round(mem.total / (1024*1024), 2)} MiB",
        "cpu_usage_percent": cpu
    }


# Add endpoint to force restart stuck tasks
@app.post("/restart_task/{task_id}")
def restart_task(task_id: str, key: str = Query(...)):
    if key != API_KEY:
        raise HTTPException(403, "Invalid API key")
        
    if task_id not in tasks:
        raise HTTPException(404, "Task not found")
    
    # Kill existing worker if running
    if task_id in active_workers:
        try:
            # Note: This is a forceful approach - in production you might want gentler handling
            thread = active_workers[task_id]
            print(f"[{task_id}] Attempting to restart stuck task")
        except:
            pass
    
    # Reset task status
    try:
        data = get_task(task_id)
        data["status"] = "waiting"
        data["stage"] = "video"
        data["progress"] = 0
        data.pop("error", None)
        set_task(task_id, data)
        
        # Try to start again
        start_task_in_background(task_id)
        
        return {"message": f"Task {task_id} restart initiated"}
    except Exception as e:
        raise HTTPException(500, f"Failed to restart task: {str(e)}")


# start cleanup thread
threading.Thread(target=cleanup_old_tasks, daemon=True).start()
