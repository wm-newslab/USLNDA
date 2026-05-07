#!/usr/bin/env python3
import argparse
import json
import os
import re
import shutil
import feedparser
import requests
import subprocess
import time
import logging
from logging.handlers import TimedRotatingFileHandler
from queue import Queue
import concurrent.futures
from tqdm import tqdm
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlsplit, urlunparse, parse_qsl, urlencode
from storysniffer import StorySniffer
from datetime import datetime, timezone, timedelta
import smtplib
from multiprocessing import Process
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import threading

sleeping_flag = threading.Event()

# === CONFIGURATION ===
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO")

def setup_logger(args):
    """Setup logging with rotation and cleanup of old logs."""
    log_dir = os.path.dirname(args.log)
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(getattr(logging, args.log_level.upper(), logging.DEBUG))

    if logger.hasHandlers():
        logger.handlers.clear()

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
    console_handler.setFormatter(console_formatter)

    file_handler = TimedRotatingFileHandler(args.log, when="D", interval=1, backupCount=2, utc=True)
    file_handler.setLevel(getattr(logging, args.log_level.upper(), logging.DEBUG))
    file_formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info(f"Logging initialized. Writing logs to {args.log}")

def cleanup_old_logs(log_path, days=2):
    """Delete log files older than N days."""
    log_dir = os.path.dirname(log_path)
    now = time.time()
    cutoff = now - (days * 86400)

    for fname in os.listdir(log_dir):
        if fname.startswith(os.path.basename(log_path)):
            fpath = os.path.join(log_dir, fname)
            if os.path.isfile(fpath) and os.path.getmtime(fpath) < cutoff:
                os.remove(fpath)
                logging.info(f"Deleted old log file: {fpath}")

def send_email(subject, body, attachment_path=None):
    """Send an email alert with optional attachment."""
    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_TO
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "plain"))

    # --- Attach log file if provided ---
    if attachment_path and os.path.exists(attachment_path):
        try:
            with open(attachment_path, "rb") as f:
                attachment = MIMEText(f.read().decode("utf-8", errors="ignore"))
                attachment.add_header(
                    "Content-Disposition",
                    f'attachment; filename="{os.path.basename(attachment_path)}"'
                )
                msg.attach(attachment)
        except Exception as e:
            logging.error(f"Failed to attach log file {attachment_path}: {e}")

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg)
        logging.info("Email sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")


def get_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="News Archival Script")
    parser.add_argument("--input", default="data.json", help="Path to JSON input file")
    parser.add_argument("--sleep", type=int, default=3600, help="Time between iterations (in seconds)")
    parser.add_argument("--max_articles", type=int, default=5, help="Maximum number of articles to scrape per publication")
    parser.add_argument("--log", default="/app1/ia-collection/news_scraper.log", help="Path to log file")
    parser.add_argument("--log_level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
    parser.add_argument("--mediatype", default="web", help="Media type for Internet Archive upload")
    parser.add_argument("--collection", default="us-local-news-data", help="Collection name of the internet archive")
    parser.add_argument("--item_identifier", default="USLNDA", help="Prefix of the item identifier")
    parser.add_argument("--time_limit", type=int, help="Time limit (in seconds) for archiving subprocess")
    parser.add_argument("--time_per_url", type=int, default=120, help="Time limit (in seconds) for archiving one article")
    parser.add_argument("--collection_directory", default="/app1/ia-collection", help="Directory to collect warc files")
    parser.add_argument("--tmp_directory", default="/app1", help="Directory to temporarily collect warc files")
    parser.add_argument("--delete_warc",type=bool, default=True, help="Delete the warc file after uploading to internet archive")
    parser.add_argument("--start", type=int, default=0, help="Start index of states to process")
    parser.add_argument("--end", type=int, default=None, help="End index (exclusive) of states to process")
    parser.add_argument('--once_per_day', type=bool, default=True, help='Run only once per day for all states')
    parser.add_argument('--workers', type=int, default=5, help='Number of workers for crawling per run')
    parser.add_argument("--rolloverSize", type=int, default=10000000000, help="Declare the rollover size")
    parser.add_argument("--hung_threshold", type=int, default=7200,
                        help="If no logs written within this many seconds, alert and exit (default 3600 = 1 hour).")
    return parser.parse_args()

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
}

def is_valid_url(url):
    """Check if URL is valid."""
    try:
        result = urlsplit(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        logging.warning(f"Invalid URL: {url}")
        return False

def normalize_rss_url(url):
    """Ensure RSS feed uses HTTPS and encode its query parameters."""
    parsed = urlparse(url)
    scheme = "https"
    encoded_query = urlencode(parse_qsl(parsed.query, keep_blank_values=True), doseq=True)
    return urlunparse((scheme, parsed.netloc, parsed.path, parsed.params, encoded_query, parsed.fragment)).replace("&", "&amp;")

def get_expanded_url(short_url):
    """Follow redirects to expand short URLs."""
    try:
        response = requests.head(short_url, allow_redirects=True, timeout=5)
        return response.url
    except requests.RequestException as e:
        logging.error(f"Error resolving URL: {short_url}: {e}")
        return short_url

def extract_article_urls_from_html(html_content, base_url):
    """Extract article URLs from HTML using BeautifulSoup."""
    soup = BeautifulSoup(html_content, 'html.parser')
    resolved_base = get_expanded_url(base_url)
    return {
        urljoin(resolved_base, link['href'])
        for link in soup.find_all("a", href=True)
    }

def rename_warc_filename(directory, archive_file_name):
    # Match pattern and replace the last underscore + number with 4-digit zero-padded version
    logging.info(f"started renaming {archive_file_name}...")
    match = re.match(r"^(.*_)(\d+)(\.warc\.gz)$", archive_file_name)
    if match:
        logging.info(f"Renaming matched for {archive_file_name}...")
        base, num, ext = match.groups()
        new_file_name = f"{base}{int(num):04}{ext}"

        source_dir = os.path.join(directory, archive_file_name)
        new_src_dir = os.path.join(directory, new_file_name)
        os.rename(source_dir, new_src_dir)
        logging.info(f"File: {archive_file_name}, renamed to: {new_file_name}")
        return new_file_name
    logging.warning(f"Renaming didn't match for {archive_file_name}...")
    return archive_file_name

def create_directories(args, item_identifier):
    tmp_directory = os.path.abspath(args.tmp_directory)
    collection_directory = os.path.join(os.path.abspath(args.collection_directory), item_identifier)

    os.makedirs(tmp_directory, exist_ok=True)
    os.makedirs(collection_directory, exist_ok=True)

    logging.info(f"Created Directories:\n - TMP: {tmp_directory}\n - Collection: {collection_directory}")
    return collection_directory, tmp_directory

def write_seed_urls(seed_urls, tmp_directory, archive_file_name):
    logging.info(f"Start writing seed URLs")
    seed_file_path = os.path.join(tmp_directory, f"{archive_file_name}.txt")
    
    filtered_urls = []
    for url in seed_urls:
        if not is_valid_url(url):
            continue
        try:
            # Follow redirects to get the final status code
            response = requests.head(url, allow_redirects=True, timeout=5)
            final_status = response.status_code
            if final_status >= 400:
                logging.warning(f"Skipping URL (bad status code {final_status}): {url}")
                continue
        except requests.RequestException:
            logging.warning(f"Skipping URL (unreachable): {url}")
            continue
        filtered_urls.append(url)

    logging.info(f"Writing {len(filtered_urls)} reachable URLs to: {seed_file_path}")
    
    with open(seed_file_path, "w") as f:
        for url in filtered_urls:
            f.write(f"{url}\n")
    
    logging.info(f"Seed file written with {len(filtered_urls)} URLs")
    return seed_file_path

def submit_crawl(archive_file_name, seed_file_path, args, retries=3):
    logging.info(f"Crawling with browsertrix for: {archive_file_name}")

    cmd = [
        "crawl",
        "--urlFile", str(seed_file_path),
        "--collection", archive_file_name,
        "--combineWARC",
        "--workers", str(args.workers),
        "--behaviorTimeout", "90",
        "--rolloverSize", str(args.rolloverSize),
        "--diskUtilization", "95",
        "--pageLoadTimeout", "90",
        "--netIdleWait", "2",
        "--logLevel", "info",
        "--scopeType", "page",
        "--blockads",
        "--headless"
    ]

    attempt = 0
    while attempt <= retries:

        proc = subprocess.run(cmd)
        code = proc.returncode

        # --- success & expected stops ---
        if code == 0:
            logging.info(f"Crawling completed successfully with the cmd: {cmd}")
            return
        elif code == 14:
            logging.info("Crawling stopped due to WARC size limit reached.")
            return
        elif code == 15:
            logging.info(f"Crawling stopped due to time limit.")
            return
        elif code == 11:
            logging.info("Crawling stopped gracefully by SIGINT.")
            return
        elif code == 13:
            logging.info("Crawling stopped forcefully by SIGTERM or repeated SIGINT.")
            return

        # --- retryable errors ---
        elif code in [1, 9, 10, 21]:
            error_map = {
                1: "Generic error (check logs)",
                9: "Crawl failed unexpectedly",
                10: "Browser crashed",
                21: "Proxy error"
            }
            logging.warning(f"[WARNING] {error_map[code]} (exit {code}). Attempt {attempt+1}/{retries}.")
            if attempt < retries:
                time.sleep(300)
                attempt += 1
                continue
            else:
                subject = f"Crawl Failed: {archive_file_name} ({error_map[code]})"
                body = (
                    f"The Browsertrix crawl for '{archive_file_name}' failed repeatedly.\n\n"
                    f"Exit Code: {code}\n"
                    f"Seed File: {seed_file_path}\n"
                    f"Command Run: {' '.join(cmd)}\n\n"
                    f"Please check logs for more details."
                )
                send_email(subject, body, attachment_path=args.log)
                os._exit(1)
        # --- fatal errors ---
        elif code in [3, 12, 16, 17]:
            fatal_map = {
                3: "Out of disk space",
                12: "Too many failed pages (failed limit reached)",
                16: "Disk utilization limit reached",
                17: "Fatal non-retryable error"
            }
            logging.error(f"[ERROR] {fatal_map[code]} (exit {code}).")
            subject = f"Crawl Failed: {archive_file_name} ({fatal_map[code]})"
            body = (
                f"The Browsertrix crawl for '{archive_file_name}' failed with a fatal error.\n\n"
                f"Exit Code: {code}\n"
                f"Reason: {fatal_map[code]}\n"
                f"Seed File: {seed_file_path}\n"
                f"Command Run: {' '.join(cmd)}\n\n"
                f"Please check logs and fix the issue before retrying."
            )
            send_email(subject, body, attachment_path=args.log)
            raise subprocess.CalledProcessError(code, cmd)

        # --- unknown codes ---
        else:
            logging.error(f"[ERROR] Crawling failed with unexpected exit code {code}.")
            subject = f"Crawl Failed: {archive_file_name} (Exit Code {code})"
            body = (
                f"The Browsertrix crawl for '{archive_file_name}' failed with an unknown exit code.\n\n"
                f"Exit Code: {code}\n"
                f"Seed File: {seed_file_path}\n"
                f"Command Run: {' '.join(cmd)}\n\n"
                f"Please check logs for more details."
            )
            send_email(subject, body, attachment_path=args.log)
            raise subprocess.CalledProcessError(code, cmd)

def move_warc(directory, archive_file_name, tmp_directory):
    """Move generated WARC.GZ files to final collection directory."""
    try:
        logging.info(f"Started moving WARC.GZ files for {archive_file_name}")
        os.makedirs(directory, exist_ok=True)
        source_dir = os.path.join(tmp_directory, 'collections', archive_file_name)
        if not os.path.exists(source_dir):
            logging.info(f"Source directory not found: {source_dir}")
            return

        for file_name in os.listdir(source_dir):
            if file_name.endswith(".warc.gz"):
                src_file = os.path.join(source_dir, file_name)
                shutil.move(src_file, directory)
                logging.info(f"Moved: {file_name} to {directory}")
    except Exception as e:
        logging.error(f"Error moving WARC.GZ files for {archive_file_name}: {e}")
        subject = f"Move Failed: {archive_file_name}"
        body = f"Failed to move WARC.GZ files to collection directory.\nException: {e}"
        send_email(subject, body)

def delete_warc_dir(archive_file_name, tmp_directory, args):
    """Delete temporary WARC.GZ directory, seed file, if enabled."""
    if not args.delete_warc:
        return

    try:
        dir_path = os.path.join(tmp_directory, 'collections', archive_file_name)
        txt_file_path = os.path.join(tmp_directory, f"{archive_file_name}.txt")

        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
            logging.info(f"Deleted directory: {dir_path}")
        else:
            logging.info(f"Directory not found: {dir_path}")

        if os.path.exists(txt_file_path):
            os.remove(txt_file_path)
            logging.info(f"Deleted file: {txt_file_path}")
        else:
            logging.info(f"File not found: {txt_file_path}")

    except Exception as e:
        logging.error(f"Cleanup failed for {archive_file_name}: {e}")
        subject = f"Cleanup Failed: {archive_file_name}"
        body = f"Failed to delete temporary WARC directory or seed file.\nException: {e}"
        send_email(subject, body)

def seconds_until_next_utc_midnight():
    now = datetime.now(timezone.utc)
    next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return (next_midnight - now).total_seconds()

def process_publication(publication, sniffer, args):
    """Process a single publication by gathering articles and archiving them."""
    website_url = publication.get("website")
    seed_urls = []

    # First try to get articles from RSS feeds
    for rss_feed_url in publication.get("rss", []):
        feed = feedparser.parse(normalize_rss_url(rss_feed_url))
        for entry in feed.entries:
            article_url = entry.link
            if article_url and sniffer.guess(article_url):
                seed_urls.append(article_url)
                if len(seed_urls) >= args.max_articles:
                    break
        if len(seed_urls) >= args.max_articles:
            break

    # If not enough from RSS, fallback to scraping the website
    if len(seed_urls) < args.max_articles:
        try:
            response = requests.get(website_url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            for article_url in extract_article_urls_from_html(response.text, website_url):
                if article_url and sniffer.guess(article_url):
                    seed_urls.append(article_url)
                    if len(seed_urls) >= args.max_articles:
                        break
        except requests.RequestException as e:
            logging.debug(f"Failed to scrape {website_url}: {e}")

    if seed_urls:
        seed_urls.append(website_url)
        return seed_urls
        
    else:
        logging.debug(f"No valid URLs for {website_url}")

def archive(seed_urls, archive_file_name, item_identifier, args):
    """Run Browsertrix Crawler as a Kubernetes Job to archive seed URLs."""
    logging.info(f"--- Starting archive job for: {archive_file_name} ---")
    
    try:
        # 1. Create directories
        collection_dir, tmp_dir = create_directories(args, item_identifier)
        
        # 2. Write seed URLs
        seed_file_path = write_seed_urls(seed_urls, tmp_dir, archive_file_name)
        
        # 3. Run Browsertrix crawl
        submit_crawl(archive_file_name, seed_file_path, args)
        
        # 4. Move WARC files to collection directory
        move_warc(collection_dir, archive_file_name, tmp_dir)
        
        # 5. Clean up temporary WARC directory & seed file
        delete_warc_dir(archive_file_name, tmp_dir, args)

        logging.info(f"--- Archive job completed for: {archive_file_name} ---\n")
    except subprocess.CalledProcessError as e:
        logging.error(f"[ERROR] Subprocess failed: {e}")
        return 0, 0

# New helper: check which states were not processed (no warc files found for that state in collection dir for the date)
def find_unprocessed_states_for_date(args, states, date_str):
    """
    Look in args.collection_directory/<item_identifier>-<date_str> for files that contain the state token.
    If the folder doesn't exist or no files match the state's token, the state is considered unprocessed.
    """
    collection_root = os.path.abspath(args.collection_directory)
    target_dir = os.path.join(collection_root, f"{args.item_identifier}-{date_str}")
    unprocessed = []
    logging.debug(f"Checking processed files in: {target_dir} for states: {states}")

    # If target directory doesn't exist, none processed
    if not os.path.isdir(target_dir):
        logging.info(f"Collection directory for date not found: {target_dir} (treating all states as unprocessed)")
        return list(states)

    files = os.listdir(target_dir)
    # For each state, check if any filename contains "-{state}-" (this matches the archive_file_name we produce)
    for state in states:
        matched = any(f"-{state}-" in fn for fn in files)
        if not matched:
            unprocessed.append(state)

    return unprocessed

# Monitor thread: watch the log file modification time; if not updated for hung_threshold seconds, send email & exit
def start_log_monitor_thread(args, stop_event):
    def monitor():
        logging.info(f"Starting log monitor: will alert if no log updates for {args.hung_threshold} seconds.")
        while not stop_event.is_set():
            try:
                if sleeping_flag.is_set():
                    stop_event.wait(timeout=min(60, args.hung_threshold // 6))
                    continue
        
                log_path = args.log
                if os.path.exists(log_path):
                    mtime = os.path.getmtime(log_path)
                    age = time.time() - mtime
                    if age > args.hung_threshold:
                        logging.error(f"No log updates for {age:.0f} seconds (threshold {args.hung_threshold}). Triggering alert & exit.")
                        # Load states and compute unprocessed for current date

                        subject = f"Hung Detector: No logs for {int(age)}s - exiting"
                        body = (
                            f"No logs have been written to '{log_path}' for {int(age)} seconds (threshold {args.hung_threshold}).\n\n"
                            f"The process will exit so Kubernetes can restart the pod.\n\n"
                            f"Please investigate the worker logs & container for more information."
                        )
                        send_email(subject, body, attachment_path=args.log)
                        # Ensure immediate stop. Use os._exit so we exit even from a thread.
                        os._exit(1)
                else:
                    logging.warning(f"Log file not found at: {args.log}")
                # check every minute (or smaller if threshold < 60)
                sleep_time = min(60, max(1, args.hung_threshold // 6))
                stop_event.wait(timeout=sleep_time)
            except Exception as e:
                logging.exception(f"Exception in log monitor thread: {e}")
                # On monitor failure, do not crash the main process; sleep briefly and continue
                stop_event.wait(timeout=30)
    t = threading.Thread(target=monitor, name="log-monitor", daemon=True)
    t.start()
    return t

def main():
    args = get_arguments()

    # Setup logger
    setup_logger(args)
    cleanup_old_logs(args.log, days=2)

    sniffer = StorySniffer()
    last_run_date = None

    stop_event = threading.Event()
    start_log_monitor_thread(args, stop_event)

    try:
        while True:
            try:
                current_date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')

                if args.once_per_day and current_date_str == last_run_date:
                    sleep_secs = seconds_until_next_utc_midnight()
                    logging.info(f"Already ran today. Sleeping until next UTC midnight ({sleep_secs:.0f} sec)...")
                    sleeping_flag.set()
                    time.sleep(sleep_secs)
                    sleeping_flag.clear()
                    continue

                with open(args.input, "r") as f:
                    data = json.load(f)

                states = list(data.keys())
                start = args.start
                end = args.end if args.end is not None else len(states)
                logging.info(f"Start is: {start}, end is: {end}")
                selected_states = states[start:end]
                logging.info(f"Selected states {len(selected_states)} are: {selected_states}")

                timestamp = datetime.now(timezone.utc)
                item_identifier = f"{args.item_identifier}-{timestamp.strftime('%Y%m%d')}"

                # After finishing all selected states for the run, compute any unprocessed states and log them
                try:
                    processed_unprocessed = find_unprocessed_states_for_date(args, selected_states, timestamp.strftime('%Y%m%d'))
                    if processed_unprocessed:
                        logging.warning(f"Unprocessed states for {timestamp.strftime('%Y-%m-%d')}: {processed_unprocessed}")
                except Exception as e:
                    logging.debug(f"Failed to compute unprocessed states: {e}")

                for state in processed_unprocessed:
                    logging.info(f"Processing state: {state}")

                    seed_urls = []
                    timestamp_state = datetime.now(timezone.utc)
                    if timestamp.strftime('%Y%m%d') != timestamp_state.strftime('%Y%m%d'):
                        break

                    archive_file_name = f"{args.item_identifier}-{state}-{timestamp.strftime('%Y%m%d')}-{timestamp.strftime('%H%M%S')}"

                    publications = data[state]

                    all_publications = []
                    for news_media in ['newspaper', 'tv', 'radio', 'broadcast']:
                        all_publications.extend([
                            pub for pub in publications.get(news_media, [])
                            if pub.get("website_status_code") in range(200, 400)
                        ])

                    # Run them all in parallel with one global tqdm bar
                    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
                        futures = [executor.submit(process_publication, pub, sniffer, args) for pub in all_publications]

                        for future in tqdm(concurrent.futures.as_completed(futures),
                                        total=len(futures),
                                        desc="Processing all publications"):
                            try:
                                publication_urls = future.result()
                                if publication_urls:
                                    seed_urls.extend(publication_urls)
                            except Exception as e:
                                logging.error(f"Error processing publication in parallel: {e}")

                    if seed_urls:
                        archive(seed_urls, archive_file_name, item_identifier, args)
                    else:
                        logging.error(f"No seed URLs collected for state: {state}. Skipping archive.")

                last_run_date = current_date_str

            except Exception as e:
                logging.error(f"Fatal error: {e}", exc_info=True)
                subject = "News Archival Script Fatal Error"
                body = f"The main archival script crashed. See the attachment"
                send_email(subject, body, attachment_path=args.log)
                os._exit(1)
    finally:
        stop_event.set()
        logging.info("Shutting down monitor thread (if running).")

if __name__ == "__main__":
    main()
