#!/usr/bin/env python3
"""
Process-based Internet Archive uploader for HPC Kubernetes.

- Uses ProcessPoolExecutor (multiple OS processes) instead of threads.
- Each worker configures its own logger (appends to same log file).
- Uses internetarchive.upload built-in retries.
- Optional staging to local ephemeral disk before upload.
- Optional deletion after successful upload.
- Email notifications on final failures.
"""

import os
import fcntl
import re
import time
import requests
import argparse
import shutil
import hashlib
import logging
import smtplib
import traceback
from datetime import datetime, timedelta, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed
from logging.handlers import TimedRotatingFileHandler
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from internetarchive import upload, get_item

# ================= Configuration (override with env or CLI args) =================
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO")

# IO buffer for hashing/streaming (4 MiB recommended)
IO_BUFFER = 4 * 1024 * 1024

# ================= Logging helpers =================
def setup_main_logger(log_path, level="INFO"):
    """Configure main process logger (console + rotating file)."""
    log_dir = os.path.dirname(log_path) or "."
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Avoid duplicate handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
    console_handler.setFormatter(console_formatter)

    file_handler = TimedRotatingFileHandler(log_path, when="D", interval=1, backupCount=7, utc=True)
    file_handler.setLevel(getattr(logging, level.upper(), logging.DEBUG))
    file_formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.debug("Main logger configured")

def setup_worker_logger(log_path, level="INFO"):
    """
    Configure logger in worker process.
    We append to the same log file (handlers are local to process).
    """
    logger = logging.getLogger()
    # If already configured in this process, skip
    if logger.handlers:
        return
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(f"[%(asctime)s] [PID %(process)d] [%(levelname)s] %(message)s")
    console_handler.setFormatter(console_formatter)

    file_handler = TimedRotatingFileHandler(log_path, when="D", interval=1, backupCount=7, utc=True)
    file_handler.setLevel(getattr(logging, level.upper(), logging.DEBUG))
    file_formatter = logging.Formatter(f"[%(asctime)s] [PID %(process)d] [%(levelname)s] %(message)s")
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.debug("Worker logger configured")

def cleanup_old_logs(log_path, days=7):
    log_dir = os.path.dirname(log_path) or "."
    now = time.time()
    cutoff = now - (days * 86400)
    base = os.path.basename(log_path)
    for fname in os.listdir(log_dir):
        if fname.startswith(base):
            fpath = os.path.join(log_dir, fname)
            try:
                if os.path.isfile(fpath) and os.path.getmtime(fpath) < cutoff:
                    os.remove(fpath)
                    logging.info(f"Deleted old log file: {fpath}")
            except Exception:
                logging.exception("Error cleaning logs")

# ================= Utilities =================
def send_email(subject, body):
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_USER
        msg['To'] = EMAIL_TO
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg)
        logging.info(f"[Email] Sent: {subject}")
    except Exception as e:
        logging.error(f"[Email] Failed to send email: {e}")

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

def compute_md5(path, bufsize=IO_BUFFER):
    """Compute md5 digest using large buffer to reduce syscall overhead."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(bufsize), b""):
            h.update(chunk)
    return h.hexdigest()

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

def get_ia_metadata(item_id):
    """Fetch metadata from Internet Archive using internetarchive.get_item fallback."""
    try:
        item = get_item(item_id)
        meta = item.item_metadata
        if isinstance(meta, dict):
            return meta
    except Exception as e:
        logging.debug(f"[Validator] internetarchive get_item meta failed: {e}")
    return {}

# ================= Upload helper (worker) =================
def upload_worker(task):
    """
    Worker entrypoint executed in separate process.
    `task` is a tuple: (item_identifier, file_path, file_name, metadata, max_retries, local_stage_path, delete_after, log_path, log_level)
    Retries failed uploads up to 3 times with 5 minutes between attempts.
    Sends failure email only if all retries fail.
    """
    (item_identifier, file_path, file_name, metadata,
     max_retries, local_stage_path, delete_after, log_path, log_level) = task

    # configure worker logger (per-process)
    setup_worker_logger(log_path, level=log_level)

    staged_path = file_path
    if local_stage_path:
        try:
            os.makedirs(local_stage_path, exist_ok=True)
            staged_path = os.path.join(local_stage_path, os.path.basename(file_path))
            if not os.path.exists(staged_path):
                logging.info(f"[Stage] copying {file_path} -> {staged_path}")
                shutil.copy2(file_path, staged_path)
            else:
                logging.info(f"[Stage] already staged: {staged_path}")
        except Exception:
            logging.exception("[Stage] staging failed; falling back to original path")
            staged_path = file_path

    attempts = 0
    success = False
    last_exception = None

    if not wait_for_ia():
        logging.warning("[Scheduler] IA unavailable after wait window. Deferring this run.")
        return

    
    while attempts < 3 and not success:
        attempts += 1
        try:
            logging.info(f"[Upload-Attempt {attempts}/3] {file_name} -> {item_identifier}")
            upload(
                item_identifier,
                files={file_name: staged_path},
                metadata=metadata,
                queue_derive=False,
                retries=max_retries,
                verbose=True,
                request_kwargs={"timeout": (60, 900)}  # larger read timeout for very large files
            )
            logging.info(f"[Upload-Success] {file_name} uploaded to {item_identifier}")

            # optional cleanup
            if delete_after:
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        logging.info(f"[Cleanup] Deleted original file {file_path}")
                    if staged_path != file_path and os.path.exists(staged_path):
                        os.remove(staged_path)
                        logging.info(f"[Cleanup] Deleted staged file {staged_path}")
                except Exception:
                    logging.exception("Error deleting files after upload")

            success = True
            break
        
        except Exception as e:
            last_exception = e
            logging.exception(f"[Upload-Failed Attempt {attempts}] {file_name} -> {item_identifier}")
            if attempts < 3:
                logging.info("[Retry] Sleeping 5 minutes before next attempt...")
                time.sleep(300)

    if not success:
        body = f"Failed to upload after 3 attempts:\n{item_identifier}/{file_name}" 
        send_email(f"Upload Failed: {item_identifier}/{file_name}", body)
        return (file_name, False, str(last_exception))

    return (file_name, True, None)

# ================= Main flow (scheduling + process pool) =================
def get_yesterday_directory(prefix="USLNDA"):
    yesterday_utc = datetime.now(timezone.utc) - timedelta(days=1)
    return f"{prefix}-{yesterday_utc.strftime('%Y%m%d')}"

def get_date():
    yesterday_utc = datetime.now(timezone.utc) - timedelta(days=1)
    return yesterday_utc.strftime("%Y-%m-%d")

def run_daily_upload(args):
    setup_main_logger(args.log, level=args.log_level)
    logging.info("[Scheduler] Starting upload run (process-based)")

    item_identifier = get_yesterday_directory(prefix=args.prefix)
    collection_dir = os.path.join(args.collection_directory, item_identifier)
    logging.info(f"[Scheduler] Target collection_dir = {collection_dir}")

    if not wait_for_ia():
        logging.warning("[Scheduler] IA unavailable after wait window. Deferring this run.")
        return
    
    if not os.path.exists(collection_dir):
        logging.error(f"[Scheduler] Directory not found: {collection_dir}")
        send_email("Upload Error", f"Directory not found: {collection_dir}")
        return

    # fetch IA metadata to skip already-loaded files
    ia_meta = get_ia_metadata(item_identifier)
    ia_files_md5 = {}
    if ia_meta and 'files' in ia_meta:
        ia_files_md5 = {f['name']: f.get('md5') for f in ia_meta['files'] if f.get('name', '').endswith('.warc.gz')}

    warc_files = sorted([f for f in os.listdir(collection_dir) if f.endswith(".warc.gz")])
    if not warc_files:
        logging.warning("[Scheduler] No WARC files found to upload.")
        return

    # build tasks (do md5 checks here in scheduler to avoid duplicate work in workers)
    tasks = []
    for fname in warc_files:
        new_fname = rename_warc_filename(collection_dir, fname)
        local_file_path = os.path.join(collection_dir, new_fname)

        if not ia_files_md5:
            upload_needed = True
        elif new_fname not in ia_files_md5:
            upload_needed = True
        else:
            try:
                local_md5 = compute_md5(local_file_path)
            except Exception:
                logging.exception(f"[Scheduler] md5 failed for {local_file_path}, will upload")
                upload_needed = True
            else:
                upload_needed = (local_md5 != ia_files_md5.get(new_fname))
        if upload_needed:
            date = get_date()
            description = f"US local news data for {date}"
            tasks.append((item_identifier, local_file_path, new_fname,
                          {'collection': args.collection, 'uploader': args.uploader, 'mediatype': args.mediatype, 'date': date, 'description': description},
                          args.max_retries, args.local_stage_path if args.stage_to_local else None,
                          args.delete_uploaded_warc, args.log, args.log_level))
            logging.info(f"[Scheduler] Queued: {new_fname}")
        else:
            logging.info(f"[Scheduler] Skipped (already uploaded): {new_fname}")

    if not tasks:
        logging.info("[Scheduler] Nothing to upload.")
        return

    # ProcessPoolExecutor
    max_workers = max(1, args.max_workers)
    logging.info(f"[Scheduler] Starting ProcessPoolExecutor with max_workers={max_workers}")

    success_count = 0
    fail_count = 0

    # Submit tasks to process pool
    with ProcessPoolExecutor(max_workers=max_workers) as pex:
        futures = {pex.submit(upload_worker, t): t for t in tasks}

        for fut in as_completed(futures):
            task = futures[fut]
            try:
                file_name, ok, err = fut.result()
                if ok:
                    logging.info(f"[Result] Success: {file_name}")
                    success_count += 1
                else:
                    logging.error(f"[Result] Failed: {file_name} -- err: {err}")
                    fail_count += 1
            except Exception:
                logging.exception("[Result] Worker raised unexpected exception")
                send_email("Uploader Worker Exception", f"Exception:\n{traceback.format_exc()}")
                fail_count += 1

    logging.info(f"[Scheduler] Upload run finished: {success_count} success, {fail_count} failed")
    if fail_count:
        send_email("Upload run finished with failures", f"{success_count} succeeded, {fail_count} failed for {item_identifier}")

# ================= CLI =================
def parse_args():
    p = argparse.ArgumentParser(description="Daily Internet Archive uploader (process-based).")
    p.add_argument("--collection", default="us-local-news-data", help="Collection name of the internet archive")
    p.add_argument("--collection_directory", default="/app1/ia-collection", help="Directory containing dated collection folders")
    p.add_argument("--uploader", default="Alexander C. Nwala <alexandernwala@gmail.com>", help="Uploader identity")
    p.add_argument("--mediatype", default="web", help="Media type for Internet Archive upload")
    p.add_argument("--delete_uploaded_warc", action="store_true", help="Delete the .warc file after successful upload")
    p.add_argument("--max_workers", type=int, default=10, help="Number of parallel worker processes per pod")
    p.add_argument("--log", default="/app1/news_scraper.log", help="Path to log file")
    p.add_argument("--log_level", default="INFO", help="Logging level")
    p.add_argument("--max_retries", type=int, default=6, help="Max retries per file (passed to internetarchive.upload)")
    p.add_argument("--backoff_base", type=float, default=2.0, help="Base sleep in seconds for exponential backoff (unused)")
    p.add_argument("--stage_to_local", action="store_true", help="Copy files to local SSD before uploading (faster reads)")
    p.add_argument("--local_stage_path", default="/tmp/ia_staging", help="Local staging directory path")
    p.add_argument("--prefix", default="USLNDA", help="Prefix for dated folder name (YESTERDAY prefix-YYYYMMDD)")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    setup_main_logger(args.log, level=args.log_level)
    cleanup_old_logs(args.log, days=7)

    logging.info("[Main] Starting persistent uploader loop (runs daily, skips sleep if overrun)")

    while True:
        run_start_date = datetime.now(timezone.utc).date()

        try:
            run_daily_upload(args)
        except Exception:
            logging.exception("Fatal error in main:")
            send_email(
                "Uploader Fatal Error. The process will exit so Kubernetes can restart the pod.",
                f"Exception:\n{traceback.format_exc()}"
            )
            os._exit(1)

        now = datetime.now(timezone.utc)
        run_end_date = now.date()

        # If the run crossed midnight UTC, immediately start next cycle
        if run_end_date != run_start_date:
            logging.info(
                "[Main] Upload run crossed midnight UTC. Starting next run immediately without sleep."
            )
            continue

        # Otherwise, sleep until midnight UTC
        tomorrow = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        sleep_seconds = (tomorrow - now).total_seconds()

        logging.info(
            f"[Main] Upload finished early. Sleeping until midnight UTC ({sleep_seconds/3600:.2f} hours)..."
        )
        time.sleep(sleep_seconds)


