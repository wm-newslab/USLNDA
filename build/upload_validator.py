#!/usr/bin/env python3
"""
High-performance Internet Archive upload validator.
- Validates uploaded files by comparing local MD5s to IA metadata.
- Uploads missing files using parallel processing.
- Retries failed uploads (3 attempts, 5 min delay between tries).
- Sleeps until next midnight after completion.
"""

import os
import json
import subprocess
import hashlib
import logging
import time
import traceback
import smtplib
import requests
import fcntl
import re
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from concurrent.futures import ProcessPoolExecutor, as_completed
from internetarchive import upload

# ================= CONFIG =================
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO")

BASE_DIR = os.getenv("BASE_DIR", "/app1/ia-collection")
DAYS_BACK = int(os.getenv("DAYS_BACK", 3))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 8))
IO_BUFFER = 4 * 1024 * 1024  # 4 MiB for fast MD5
MAX_ATTEMPTS = 3
SLEEP_BETWEEN_ATTEMPTS = 300  # 5 min
COLLECTION = "us-local-news-data"
UPLOADER = os.getenv("BASE_DIR")
MEDIA_TYPE = "web"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ================= EMAIL =================
def send_email(subject, body):
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_USER
        msg["To"] = EMAIL_TO
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg)

        logging.info(f"[Email] Sent: {subject}")
    except Exception as e:
        logging.error(f"[Email] Failed: {e}")

# ================= UTILITIES =================
def compute_md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(IO_BUFFER), b""):
            h.update(chunk)
    return h.hexdigest()

def get_ia_metadata(item_id):
    """Fetch IA metadata using subprocess for reliability."""
    try:
        result = subprocess.run(
            ["ia", "metadata", item_id],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error(f"[Validator] Metadata fetch failed for {item_id}: {e.stderr}")
        return None

def rename_warc_filename(directory, archive_file_name):
    match = re.match(r"^(.*_)(\d+)(\.warc\.gz)$", archive_file_name)
    if not match:
        return archive_file_name

    base, num, ext = match.groups()
    new_file_name = f"{base}{int(num):04}{ext}"

    old_path = os.path.join(directory, archive_file_name)
    new_path = os.path.join(directory, new_file_name)

    if old_path == new_path or os.path.exists(new_path):
        return new_file_name

    lock_path = old_path + ".lock"

    renamed = False
    with open(lock_path, "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if os.path.exists(old_path) and not os.path.exists(new_path):
            os.rename(old_path, new_path)
            logging.info(f"[Rename] {archive_file_name} -> {new_file_name}")
            renamed = True

    if renamed:
        try:
            os.unlink(lock_path)
        except FileNotFoundError:
            pass

    return new_file_name

def ia_is_available(timeout=10):
    try:
        r = requests.get("https://archive.org", timeout=timeout)
        return r.status_code < 500
    except Exception:
        return False

def wait_for_ia(initial_delay=10, max_delay=3600, factor=2):
    delay = initial_delay
    while not ia_is_available():
        logging.warning(f"[Scheduler] IA down. Waiting {delay} seconds...")
        time.sleep(delay)
        delay = min(delay * factor, max_delay)
    return True

def get_date(days_back=DAYS_BACK):
    target_date = datetime.now() - timedelta(days=days_back)
    return target_date.strftime("%Y-%m-%d")

# ================= UPLOAD WORKER =================
def upload_worker(task):
    """Executed in a separate process."""

    if not wait_for_ia():
        logging.warning("[Scheduler] IA unavailable after wait window. Deferring this run.")
        return

    item_id, file_path, file_name = task
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            logging.info(f"[Upload-Attempt {attempt}] {file_name} -> {item_id}")
            date = get_date()
            description = f"US local news data for {date}"
            upload(
                item_id,
                files={file_name: file_path},
                metadata={
                    "collection": COLLECTION,
                    "uploader": UPLOADER,
                    "mediatype": MEDIA_TYPE,
                    "date": date, 
                    "description": description
                },
                queue_derive=False,
                retries=6,
                verbose=True,
                request_kwargs={"timeout": (60, 900)},
            )
            logging.info(f"[Upload-Success] {file_name} uploaded successfully.")
            return (file_name, True, None)
        except Exception:
            err = traceback.format_exc()
            logging.error(f"[Upload-Failed {attempt}] {file_name}: {err}")
            if attempt < MAX_ATTEMPTS:
                logging.info(f"[Retry] Sleeping {SLEEP_BETWEEN_ATTEMPTS/60:.0f} min before retry.")
                time.sleep(SLEEP_BETWEEN_ATTEMPTS)
            else:
                return (file_name, False, err)

# ================= VALIDATOR =================
class UploadValidator:
    def __init__(self, base_dir=BASE_DIR, days_back=DAYS_BACK):
        self.base_dir = base_dir
        self.days_back = days_back

    def _validate_and_prepare_tasks(self, local_dir, metadata):
        """Find missing or mismatched files for upload."""
        ia_files_md5 = {
            f["name"]: f.get("md5")
            for f in metadata.get("files", [])
            if f.get("name", "").endswith(".warc.gz")
        }

        tasks = []
        mismatched = []

        for aname in os.listdir(local_dir):

            if not aname.endswith(".warc.gz"):
                continue
            
            fname = rename_warc_filename(local_dir, aname)
            local_path = os.path.join(local_dir, fname)

            if fname not in ia_files_md5:
                logging.warning(f"[Validator] Missing in IA: {fname}")
                tasks.append((os.path.basename(local_dir), local_path, fname))
                continue

            try:
                local_md5 = compute_md5(local_path)
                if local_md5 == ia_files_md5[fname]:
                    os.remove(local_path)
                    logging.info(f"[Validator] Deleted verified file: {local_path}")
                else:
                    logging.warning(f"[Validator] MD5 mismatch: {fname}")
                    mismatched.append(local_path)
            except Exception:
                logging.exception(f"[Validator] Failed MD5 for {fname}")

        return tasks, mismatched

    def run_once(self):
        """Validate one day’s collection and upload missing files."""
        target_date = datetime.now() - timedelta(days=self.days_back)
        folder_name = f"USLNDA-{target_date.strftime('%Y%m%d')}"
        folder_path = os.path.join(self.base_dir, folder_name)

        logging.info(f"[Validator] Checking folder: {folder_path}")
        if not os.path.isdir(folder_path):
            logging.warning(f"[Validator] Directory does not exist: {folder_path}")
            return
        
        if not wait_for_ia():
            logging.warning("[Scheduler] IA unavailable after wait window. Deferring this run.")
            return

        metadata = get_ia_metadata(folder_name)
        if not metadata:
            logging.error(f"[Validator] Failed to fetch IA metadata for {folder_name}")
            return

        tasks, mismatched = self._validate_and_prepare_tasks(folder_path, metadata)

        uploaded = []
        failed = []

        if tasks:
            logging.info(f"[Validator] Starting parallel uploads ({len(tasks)} files)...")
            with ProcessPoolExecutor(max_workers=MAX_WORKERS) as pool:
                futures = {pool.submit(upload_worker, t): t for t in tasks}
                for fut in as_completed(futures):
                    fname, ok, err = fut.result()
                    local_path = os.path.join(folder_path, fname)
                    if ok:
                        uploaded.append(fname)
                        # Delete successfully uploaded file
                        try:
                            if os.path.exists(local_path):
                                os.remove(local_path)
                                logging.info(f"[Validator] Deleted uploaded file: {local_path}")
                        except Exception:
                            logging.exception(f"[Validator] Failed to delete uploaded file: {local_path}")
                    else:
                        failed.append((fname, err))

        # Send reports
        if mismatched:
            send_email(
                f"MD5 mismatch in {folder_name}",
                "\n".join(mismatched),
            )

        if uploaded:
            send_email(
                f"Uploaded missing files for {folder_name}",
                "\n".join(uploaded),
            )

        if failed:
            body = "\n\n".join([f"{f}: {e}" for f, e in failed])
            send_email(
                f"Failed uploads after retries ({folder_name}). The process will exit so Kubernetes can restart the pod.",
                f"The following uploads failed after {MAX_ATTEMPTS} attempts:\n\n{body}",
            )
            os._exit(1)

        if not mismatched and not uploaded and not failed:
            logging.info(f"[Validator] All files up to date for {folder_name}.")

        # Clean up empty folder
        try:
            if os.path.isdir(folder_path) and not os.listdir(folder_path):
                os.rmdir(folder_path)
                logging.info(f"[Validator] Deleted empty directory: {folder_path}")
        except Exception:
            logging.exception(f"[Validator] Failed to delete {folder_path}")

# ================= MAIN LOOP =================
def sleep_until_midnight():
    """Sleep until next midnight UTC."""
    now = datetime.utcnow()
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    sleep_seconds = (tomorrow - now).total_seconds()
    logging.info(f"[Scheduler] Sleeping until midnight ({sleep_seconds/3600:.2f} hours)")
    time.sleep(sleep_seconds)

if __name__ == "__main__":
    validator = UploadValidator()
    while True:
        logging.info("=== Starting validation cycle ===")
        try:
            validator.run_once()
        except Exception:
            logging.exception("[Validator] Fatal error during validation")
            send_email("Validator Fatal Error", traceback.format_exc())
        sleep_until_midnight()
