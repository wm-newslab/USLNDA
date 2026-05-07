#!/usr/bin/env python3

import argparse
import concurrent.futures
import datetime as dt
import json
import os
import subprocess
import sys
import threading
import time
from typing import Dict, List, Optional, Set, Tuple

from internetarchive import get_item


print_lock = threading.Lock()


def log(msg: str) -> None:
    with print_lock:
        ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] {msg}", flush=True)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download only missing .warc.gz files for USLNDA Internet Archive identifiers."
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--download_root", required=True)
    parser.add_argument("--ia_bin", default="ia")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry_sleep", type=int, default=5)
    parser.add_argument("--progress_log_root", default="download_logs")
    parser.add_argument("--start_identifier", required=True)
    parser.add_argument("--end_identifier", required=True)
    parser.add_argument("--max_identifiers", type=int, default=None)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Redownload all remote warc.gz files for an identifier even if already present locally."
    )
    return parser.parse_args()


def parse_identifier_date(identifier: str) -> dt.date:
    """
    Expected form: USLNDA-YYYYMMDD
    """
    try:
        _, datestr = identifier.split("-", 1)
        return dt.datetime.strptime(datestr, "%Y%m%d").date()
    except Exception as e:
        raise ValueError(f"Invalid identifier format: {identifier}") from e


def make_identifier(day: dt.date) -> str:
    return f"USLNDA-{day.strftime('%Y%m%d')}"


def generate_identifiers(
    year: int,
    month: int,
    start_identifier: str,
    end_identifier: str,
    max_identifiers: Optional[int] = None,
) -> List[str]:
    start_day = parse_identifier_date(start_identifier)
    end_day = parse_identifier_date(end_identifier)

    if start_day > end_day:
        raise ValueError("start_identifier must be <= end_identifier")

    if start_day.year != year or start_day.month != month:
        raise ValueError("start_identifier does not match --year/--month")

    if end_day.year != year or end_day.month != month:
        raise ValueError("end_identifier does not match --year/--month")

    out = []
    cur = start_day
    while cur <= end_day:
        out.append(make_identifier(cur))
        cur += dt.timedelta(days=1)

    if max_identifiers is not None:
        out = out[:max_identifiers]

    return out


def get_remote_warc_filenames(identifier: str) -> List[str]:
    """
    Return remote .warc.gz filenames listed by Internet Archive for this identifier.
    """
    item = get_item(identifier)

    remote_files = []
    for f in item.files:
        name = f.get("name")
        if name and name.endswith(".warc.gz"):
            remote_files.append(name)

    return sorted(remote_files)


def get_local_warc_filenames(identifier_dir: str) -> Set[str]:
    """
    Return local .warc.gz filenames present in identifier_dir.
    """
    if not os.path.isdir(identifier_dir):
        return set()

    return {
        name
        for name in os.listdir(identifier_dir)
        if name.endswith(".warc.gz") and os.path.isfile(os.path.join(identifier_dir, name))
    }


def write_json(path: str, obj: Dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)


def append_jsonl(path: str, obj: Dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, sort_keys=True) + "\n")


def run_cmd(cmd: List[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True)


def download_one_file(
    identifier: str,
    filename: str,
    identifier_dir: str,
    ia_bin: str,
    retries: int,
    retry_sleep: int,
) -> Tuple[bool, str]:
    """
    Download a single file directly into identifier_dir.
    """
    ensure_dir(identifier_dir)

    target_path = os.path.join(identifier_dir, filename)
    if os.path.isfile(target_path):
        return True, "already_present"

    for attempt in range(1, retries + 1):
        cmd = [
            ia_bin,
            "download",
            identifier,
            filename,
            "--destdir",
            identifier_dir,
            "--no-directories",
        ]

        log(f"{identifier}: downloading {filename} (attempt {attempt}/{retries})")
        result = run_cmd(cmd)

        if result.returncode == 0 and os.path.isfile(target_path):
            return True, "downloaded"

        if result.returncode != 0:
            log(f"{identifier}: failed {filename} on attempt {attempt}/{retries}")
            if result.stdout:
                log(f"{identifier}: STDOUT for {filename}:\n{result.stdout.strip()}")
            if result.stderr:
                log(f"{identifier}: STDERR for {filename}:\n{result.stderr.strip()}")
        else:
            log(
                f"{identifier}: command returned success for {filename}, "
                f"but file not found at expected path {target_path}"
            )

        if attempt < retries:
            time.sleep(retry_sleep)

    return False, "failed"


def process_identifier(
    identifier: str,
    download_root: str,
    ia_bin: str,
    retries: int,
    retry_sleep: int,
    progress_log_root: str,
    force: bool,
) -> Dict:
    identifier_dir = os.path.join(download_root, identifier)
    ensure_dir(identifier_dir)
    ensure_dir(progress_log_root)

    progress_path = os.path.join(progress_log_root, f"{identifier}.json")

    result: Dict = {
        "identifier": identifier,
        "status": None,
        "remote_count": 0,
        "local_count_before": 0,
        "local_count_after": 0,
        "downloaded_now": 0,
        "already_present_count": 0,
        "failed_files": [],
        "missing_before": [],
        "missing_after": [],
    }

    try:
        remote_files = get_remote_warc_filenames(identifier)
        local_files_before = get_local_warc_filenames(identifier_dir)

        result["remote_count"] = len(remote_files)
        result["local_count_before"] = len(local_files_before)

        if not remote_files:
            result["status"] = "no_remote_warcs"
            write_json(progress_path, result)
            log(f"{identifier}: no remote .warc.gz files found")
            return result

        if force:
            files_to_download = remote_files
            result["missing_before"] = remote_files
            log(
                f"{identifier}: force mode enabled, will redownload/check all "
                f"{len(remote_files)} remote files"
            )
        else:
            missing_files = [f for f in remote_files if f not in local_files_before]
            result["missing_before"] = missing_files
            result["already_present_count"] = len(remote_files) - len(missing_files)

            if not missing_files:
                result["status"] = "already_complete"
                result["local_count_after"] = len(local_files_before)
                result["missing_after"] = []
                write_json(progress_path, result)
                log(f"{identifier}: already complete with {len(remote_files)} files")
                return result

            log(
                f"{identifier}: local {len(local_files_before)}/{len(remote_files)} files present, "
                f"downloading {len(missing_files)} missing files"
            )
            files_to_download = missing_files

        downloaded_now = 0
        failed_files: List[str] = []

        for filename in files_to_download:
            if not force and filename in local_files_before:
                continue

            ok, reason = download_one_file(
                identifier=identifier,
                filename=filename,
                identifier_dir=identifier_dir,
                ia_bin=ia_bin,
                retries=retries,
                retry_sleep=retry_sleep,
            )

            if ok and reason == "downloaded":
                downloaded_now += 1
            elif not ok:
                failed_files.append(filename)

        local_files_after = get_local_warc_filenames(identifier_dir)
        missing_after = [f for f in remote_files if f not in local_files_after]

        result["downloaded_now"] = downloaded_now
        result["failed_files"] = failed_files
        result["local_count_after"] = len(local_files_after)
        result["missing_after"] = missing_after

        if not missing_after:
            result["status"] = "completed"
        elif len(local_files_after) > 0:
            result["status"] = "partial"
        else:
            result["status"] = "failed"

        write_json(progress_path, result)
        log(
            f"{identifier}: done with status={result['status']} "
            f"local_after={result['local_count_after']} remote={result['remote_count']} "
            f"downloaded_now={downloaded_now} failed={len(failed_files)}"
        )
        return result

    except Exception as e:
        result["status"] = "exception"
        result["error"] = repr(e)
        write_json(progress_path, result)
        log(f"{identifier}: exception: {e}")
        return result


def main() -> int:
    args = parse_args()

    ensure_dir(args.download_root)
    ensure_dir(args.progress_log_root)

    identifiers = generate_identifiers(
        year=args.year,
        month=args.month,
        start_identifier=args.start_identifier,
        end_identifier=args.end_identifier,
        max_identifiers=args.max_identifiers,
    )

    if not identifiers:
        log("No identifiers generated. Exiting.")
        return 0

    log(f"Generated {len(identifiers)} identifiers")
    for ident in identifiers:
        log(f"  {ident}")

    summary_jsonl = os.path.join(args.progress_log_root, "summary.jsonl")
    if os.path.exists(summary_jsonl):
        os.remove(summary_jsonl)

    results: List[Dict] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_identifier = {
            executor.submit(
                process_identifier,
                identifier,
                args.download_root,
                args.ia_bin,
                args.retries,
                args.retry_sleep,
                args.progress_log_root,
                args.force,
            ): identifier
            for identifier in identifiers
        }

        for future in concurrent.futures.as_completed(future_to_identifier):
            identifier = future_to_identifier[future]
            try:
                res = future.result()
            except Exception as e:
                res = {
                    "identifier": identifier,
                    "status": "future_exception",
                    "error": repr(e),
                }
                log(f"{identifier}: future exception: {e}")

            results.append(res)
            append_jsonl(summary_jsonl, res)

    total = len(results)
    completed = sum(1 for r in results if r.get("status") in {"completed", "already_complete"})
    partial = sum(1 for r in results if r.get("status") == "partial")
    failed = sum(1 for r in results if r.get("status") in {"failed", "exception", "future_exception"})
    no_remote = sum(1 for r in results if r.get("status") == "no_remote_warcs")

    total_downloaded_now = sum(int(r.get("downloaded_now", 0)) for r in results)

    summary = {
        "total_identifiers": total,
        "completed_or_already_complete": completed,
        "partial": partial,
        "failed": failed,
        "no_remote_warcs": no_remote,
        "downloaded_now_total": total_downloaded_now,
    }

    summary_path = os.path.join(args.progress_log_root, "final_summary.json")
    write_json(summary_path, summary)

    log("Final summary:")
    log(json.dumps(summary, indent=2, sort_keys=True))

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())