#!/usr/bin/env python3
import argparse
import calendar
import csv
import gzip
import hashlib
import os
import re
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlparse
from typing import Tuple, Optional, Dict, Any, List

import pandas as pd
from bs4 import BeautifulSoup
from storysniffer import StorySniffer
from warcio.archiveiterator import ArchiveIterator


sniffer = StorySniffer()


def ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{ts()}Z] {msg}", flush=True)


def normalize_hostname(value: str) -> str:
    if value is None:
        return ""
    value = str(value).strip().lower()
    if not value:
        return ""
    if not re.match(r"^[a-z]+://", value):
        value = "http://" + value
    parsed = urlparse(value)
    host = parsed.netloc.lower().strip()
    if host.startswith("www."):
        host = host[4:]
    host = host.split(":")[0]
    return host


def sanitize_path_component(value: str) -> str:
    value = str(value).strip()
    value = value.replace("/", "_")
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", value)
    return value.strip("_") or "unknown"


def safe_text(value):
    if pd.isna(value):
        return None
    return str(value).strip()


def host_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower().split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    return host


def load_site_mapping(csv_path: str) -> Dict[str, Dict[str, Dict[str, Any]]]:
    log(f"Loading site CSV: {csv_path}")
    df = pd.read_csv(csv_path)

    required_cols = [
        "state_code",
        "source",
        "website",
        "city-county-long",
        "city-county-lat",
        "fips",
        "county",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise SystemExit(f"CSV missing required columns: {missing}")

    state_to_host_map = defaultdict(dict)

    total_rows = 0
    kept_rows = 0
    for _, row in df.iterrows():
        total_rows += 1
        state_code = safe_text(row.get("state_code"))
        website = safe_text(row.get("website"))

        if not state_code or not website:
            continue

        state_code = state_code.upper()
        host = normalize_hostname(website)
        if not host:
            continue

        meta = {
            "state_code": state_code,
            "source": safe_text(row.get("source")),
            "website": safe_text(row.get("website")),
            "city_county_long": row.get("city-county-long"),
            "city_county_lat": row.get("city-county-lat"),
            "fips": safe_text(row.get("fips")),
            "county": safe_text(row.get("county")),
        }

        state_to_host_map[state_code][host] = meta
        kept_rows += 1

    log(f"CSV rows read: {total_rows}")
    log(f"CSV usable website mappings: {kept_rows}")
    log(f"States in mapping: {sorted(state_to_host_map.keys())}")
    return state_to_host_map


def extract_title_and_text(html: str) -> Tuple[Optional[str], str]:
    soup = BeautifulSoup(html, "lxml")

    title = None
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    paragraphs = soup.find_all("p")
    text = "\n".join(
        p.get_text(" ", strip=True)
        for p in paragraphs
        if p.get_text(" ", strip=True)
    ).strip()

    return title, text


def is_news_article(url: str, html: str) -> bool:
    lowered = (url or "").lower()

    if lowered.endswith((
        ".jpg", ".jpeg", ".png", ".gif", ".webp",
        ".svg", ".css", ".js", ".woff", ".woff2",
        ".ttf", ".ico", ".xml", ".json", ".rss"
    )):
        return False

    if any(x in lowered for x in (
        "/wp-content/", "/uploads/", "/static/", "/assets/", "/images/"
    )):
        return False

    if not sniffer.guess(url):
        return False

    soup = BeautifulSoup(html, "lxml")

    if not soup.title or not soup.title.string:
        return False

    if soup.find("meta", property="article:published_time"):
        return True

    if soup.find("script", type="application/ld+json"):
        return True

    return True


def make_unique_id(url: str, crawl_date: str, state_code: str, fips: str, county: str) -> str:
    base = f"{url}|{crawl_date}|{state_code}|{fips}|{county}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def flush_partition_records(
    records: List[Dict[str, Any]],
    output_root: str,
    state_code: str,
    county: str,
    year: str,
    month: str,
    day: str,
    part_name: str,
) -> None:
    if not records:
        return

    out_dir = os.path.join(
        output_root,
        sanitize_path_component(state_code),
        sanitize_path_component(county),
        year,
        month,
        day,
    )
    os.makedirs(out_dir, exist_ok=True)

    df = pd.DataFrame(records)
    out_path = os.path.join(out_dir, part_name)
    df.to_parquet(out_path, index=False)
    log(f"Wrote {len(df)} rows -> {out_path}")


def flush_all_buffers(
    buffers: Dict[Any, List[Dict[str, Any]]],
    partition_write_counts: Dict[Any, int],
    output_root: str,
    warc_stem: str,
) -> None:
    for partition_key, records in buffers.items():
        if not records:
            continue

        state_code, county, year, month, day = partition_key
        write_idx = partition_write_counts[partition_key]
        part_name = f"{warc_stem}_part-{write_idx:05d}.parquet"

        flush_partition_records(
            records=records,
            output_root=output_root,
            state_code=state_code,
            county=county,
            year=year,
            month=month,
            day=day,
            part_name=part_name,
        )
        partition_write_counts[partition_key] += 1
        buffers[partition_key].clear()


WARC_NAME_RE = re.compile(
    r"^USLNDA-([A-Z]{2})-(\d{8})-\d{6}_\d{4}\.warc\.gz$"
)


def list_warc_files_for_identifier(workdir: str) -> List[str]:
    if not os.path.isdir(workdir):
        raise SystemExit(f"Workdir does not exist: {workdir}")

    warcs = []
    for fname in os.listdir(workdir):
        if fname.endswith(".warc.gz"):
            warcs.append(os.path.join(workdir, fname))

    warcs.sort()
    log(f"WARC files discovered in {workdir}: {len(warcs)}")
    return warcs


def process_warc_file(
    warc_path: str,
    state_to_host_map: Dict[str, Dict[str, Dict[str, Any]]],
    output_root: str,
    buffer_limit: int,
    report_every_records: int,
    report_every_html: int,
) -> Dict[str, Any]:
    fname = os.path.basename(warc_path)
    m = WARC_NAME_RE.match(fname)
    if not m:
        log(f"Skipping file with unexpected name format: {fname}")
        return {
            "warc_file": fname,
            "state_code": None,
            "date": None,
            "records_total": 0,
            "html_responses": 0,
            "host_matches": 0,
            "valid_articles": 0,
            "error": "bad_filename",
        }

    state_code = m.group(1)
    yyyymmdd = m.group(2)
    year = yyyymmdd[:4]
    month = yyyymmdd[4:6]
    day = yyyymmdd[6:8]

    relevant_hosts = state_to_host_map.get(state_code, {})
    if not relevant_hosts:
        log(f"No website mappings for state {state_code}. Skipping {fname}")
        return {
            "warc_file": fname,
            "state_code": state_code,
            "date": yyyymmdd,
            "records_total": 0,
            "html_responses": 0,
            "host_matches": 0,
            "valid_articles": 0,
            "error": "no_mapping",
        }

    log("------------------------------------------------------------")
    log(f"Processing WARC: {warc_path}")
    log(f"Parsed from filename -> state={state_code}, date={yyyymmdd}")
    log(f"Relevant website hostnames for state {state_code}: {len(relevant_hosts)}")

    buffers = defaultdict(list)

    records_total = 0
    html_responses = 0
    host_matches = 0
    valid_articles = 0

    skipped_non_response = 0
    skipped_no_headers = 0
    skipped_non_html = 0
    skipped_host_miss = 0
    skipped_not_article = 0
    skipped_empty_content = 0

    warc_stem = os.path.basename(warc_path).replace(".warc.gz", "")
    partition_write_counts = defaultdict(int)

    t0 = time.time()
    error = None

    try:
        with gzip.open(warc_path, "rb") as stream:
            for record in ArchiveIterator(stream):
                records_total += 1

                if record.rec_type != "response":
                    skipped_non_response += 1
                    if records_total % report_every_records == 0:
                        elapsed = time.time() - t0
                        log(
                            f"{fname} | records_total={records_total} html_responses={html_responses} "
                            f"host_matches={host_matches} valid_articles={valid_articles} "
                            f"elapsed={elapsed:.1f}s"
                        )
                    continue

                if not record.http_headers:
                    skipped_no_headers += 1
                    continue

                content_type = record.http_headers.get_header("Content-Type", "") or ""
                if "text/html" not in content_type.lower():
                    skipped_non_html += 1
                    continue

                html_responses += 1

                url = record.rec_headers.get_header("WARC-Target-URI") or ""
                host = host_from_url(url)

                site_meta = relevant_hosts.get(host)
                if site_meta is None:
                    skipped_host_miss += 1
                    continue

                host_matches += 1

                try:
                    payload = record.content_stream().read()
                    html = payload.decode("utf-8", errors="replace")
                except Exception as e:
                    log(f"Payload decode error for URL={url}: {repr(e)}")
                    continue

                try:
                    if not is_news_article(url, html):
                        skipped_not_article += 1
                        continue
                except Exception as e:
                    log(f"is_news_article failed for URL={url}: {repr(e)}")
                    continue

                title, content = extract_title_and_text(html)
                if not content:
                    skipped_empty_content += 1
                    continue

                valid_articles += 1

                record_date = record.rec_headers.get_header("WARC-Date") or f"{year}-{month}-{day}T00:00:00Z"
                fips = site_meta["fips"] or "unknown"
                county = site_meta["county"] or "unknown"
                unique_id = make_unique_id(url, record_date, state_code, fips, county)

                out_row = {
                    "unique_id": unique_id,
                    "article_url": url,
                    "title": title,
                    "content": content,
                    "state_code": state_code,
                    "source": site_meta["source"],
                    "website": site_meta["website"],
                    "city-county-long": site_meta["city_county_long"],
                    "city-county-lat": site_meta["city_county_lat"],
                    "fips": fips,
                    "county": county,
                    "identifier_date": yyyymmdd,
                    "warc_file": fname,
                }

                partition_key = (state_code, county, year, month, day)
                buffers[partition_key].append(out_row)

                if len(buffers[partition_key]) >= buffer_limit:
                    write_idx = partition_write_counts[partition_key]
                    part_name = f"{warc_stem}_part-{write_idx:05d}.parquet"
                    flush_partition_records(
                        records=buffers[partition_key],
                        output_root=output_root,
                        state_code=state_code,
                        county=county,
                        year=year,
                        month=month,
                        day=day,
                        part_name=part_name,
                    )
                    partition_write_counts[partition_key] += 1
                    buffers[partition_key].clear()

                if html_responses % report_every_html == 0:
                    elapsed = time.time() - t0
                    log(
                        f"{fname} | html_responses={html_responses} host_matches={host_matches} "
                        f"valid_articles={valid_articles} elapsed={elapsed:.1f}s"
                    )

    except (EOFError, gzip.BadGzipFile, OSError) as e:
        error = repr(e)
        log(f"BAD WARC: {fname} | error={error}")

    flush_all_buffers(
        buffers=buffers,
        partition_write_counts=partition_write_counts,
        output_root=output_root,
        warc_stem=warc_stem,
    )

    elapsed = time.time() - t0
    if error is None:
        log(
            f"Finished {fname} | records_total={records_total} html_responses={html_responses} "
            f"host_matches={host_matches} valid_articles={valid_articles} elapsed={elapsed:.2f}s"
        )
    else:
        log(
            f"Finished with error {fname} | records_total={records_total} html_responses={html_responses} "
            f"host_matches={host_matches} valid_articles={valid_articles} elapsed={elapsed:.2f}s"
        )

    log(
        f"Skips: non_response={skipped_non_response}, no_headers={skipped_no_headers}, "
        f"non_html={skipped_non_html}, host_miss={skipped_host_miss}, "
        f"not_article={skipped_not_article}, empty_content={skipped_empty_content}"
    )

    return {
        "warc_file": fname,
        "state_code": state_code,
        "date": yyyymmdd,
        "records_total": records_total,
        "html_responses": html_responses,
        "host_matches": host_matches,
        "valid_articles": valid_articles,
        "error": error,
    }


def append_bad_files_csv(bad_csv_path: str, identifier: str, bad_files: List[Tuple[str, str]]) -> None:
    if not bad_files:
        return

    bad_dir = os.path.dirname(bad_csv_path)
    if bad_dir:
        os.makedirs(bad_dir, exist_ok=True)

    write_header = not os.path.exists(bad_csv_path)
    with open(bad_csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["identifier", "warc_file", "error"],
        )
        if write_header:
            writer.writeheader()
        for warc_file, error in bad_files:
            writer.writerow({
                "identifier": identifier,
                "warc_file": warc_file,
                "error": error,
            })


def process_identifier(
    identifier: str,
    download_root: str,
    state_to_host_map: Dict[str, Dict[str, Dict[str, Any]]],
    output_root: str,
    summary_csv_path: str,
    buffer_limit: int,
    report_every_records: int,
    report_every_html: int,
    workers: int,
) -> None:
    log("============================================================")
    log(f"Starting identifier: {identifier}")

    workdir = os.path.join(download_root, identifier)
    log(f"Using workdir: {workdir}")

    warc_files = list_warc_files_for_identifier(workdir)
    if not warc_files:
        log(f"No .warc.gz files found for {identifier}. Skipping.")
        return

    summary_rows = []
    total_html = 0
    total_host_matches = 0
    total_valid = 0
    bad_files: List[Tuple[str, str]] = []

    actual_workers = max(1, min(workers, len(warc_files)))
    log(f"Processing {len(warc_files)} WARC files with workers={actual_workers}")

    if actual_workers == 1:
        for i, warc_path in enumerate(warc_files, start=1):
            log(f"[{i}/{len(warc_files)}] {os.path.basename(warc_path)}")
            stats = process_warc_file(
                warc_path=warc_path,
                state_to_host_map=state_to_host_map,
                output_root=output_root,
                buffer_limit=buffer_limit,
                report_every_records=report_every_records,
                report_every_html=report_every_html,
            )

            summary_rows.append({
                "identifier": identifier,
                "warc_file": stats["warc_file"],
                "state_code": stats["state_code"],
                "date": stats["date"],
                "records_total": stats["records_total"],
                "html_responses": stats["html_responses"],
                "host_matches": stats["host_matches"],
                "valid_articles": stats["valid_articles"],
            })
            total_html += stats["html_responses"]
            total_host_matches += stats["host_matches"]
            total_valid += stats["valid_articles"]

            if stats.get("error"):
                bad_files.append((stats["warc_file"], stats["error"]))
    else:
        with ProcessPoolExecutor(max_workers=actual_workers) as executor:
            future_to_warc = {
                executor.submit(
                    process_warc_file,
                    warc_path,
                    state_to_host_map,
                    output_root,
                    buffer_limit,
                    report_every_records,
                    report_every_html,
                ): warc_path
                for warc_path in warc_files
            }

            done_count = 0
            for fut in as_completed(future_to_warc):
                warc_path = future_to_warc[fut]
                fname = os.path.basename(warc_path)

                try:
                    stats = fut.result()
                except Exception as e:
                    log(f"FAILED WARC: {fname} | error={repr(e)}")
                    bad_files.append((fname, repr(e)))
                    done_count += 1
                    continue

                done_count += 1
                log(
                    f"Completed {done_count}/{len(warc_files)} for {identifier}: "
                    f"{stats['warc_file']} valid_articles={stats['valid_articles']}"
                )

                summary_rows.append({
                    "identifier": identifier,
                    "warc_file": stats["warc_file"],
                    "state_code": stats["state_code"],
                    "date": stats["date"],
                    "records_total": stats["records_total"],
                    "html_responses": stats["html_responses"],
                    "host_matches": stats["host_matches"],
                    "valid_articles": stats["valid_articles"],
                })
                total_html += stats["html_responses"]
                total_host_matches += stats["host_matches"]
                total_valid += stats["valid_articles"]

                if stats.get("error"):
                    bad_files.append((stats["warc_file"], stats["error"]))

    summary_dir = os.path.dirname(summary_csv_path)
    if summary_dir:
        os.makedirs(summary_dir, exist_ok=True)

    write_header = not os.path.exists(summary_csv_path)
    with open(summary_csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "identifier",
                "warc_file",
                "state_code",
                "date",
                "records_total",
                "html_responses",
                "host_matches",
                "valid_articles",
            ],
        )
        if write_header:
            writer.writeheader()
        writer.writerows(summary_rows)

    bad_csv_path = os.path.join(
        os.path.dirname(summary_csv_path) or ".",
        "bad_warc_files.csv",
    )
    append_bad_files_csv(
        bad_csv_path=bad_csv_path,
        identifier=identifier,
        bad_files=bad_files,
    )

    if bad_files:
        log(f"Bad WARC files for {identifier}: {len(bad_files)}")
        for fname, err in bad_files:
            log(f"  BAD: {fname} | {err}")
    else:
        log(f"No bad WARC files for {identifier}")

    log(
        f"Identifier done: {identifier} | total_html={total_html} "
        f"total_host_matches={total_host_matches} total_valid_articles={total_valid}"
    )


def iter_identifiers_for_month(
    year: int,
    month: int,
    start_identifier: Optional[str] = None,
    end_identifier: Optional[str] = None,
):
    ndays = calendar.monthrange(year, month)[1]

    identifiers = []
    for day in range(1, ndays + 1):
        ident = f"USLNDA-{year:04d}{month:02d}{day:02d}"
        identifiers.append(ident)

    if start_identifier:
        identifiers = [x for x in identifiers if x >= start_identifier]

    if end_identifier:
        identifiers = [x for x in identifiers if x <= end_identifier]

    for ident in identifiers:
        yield ident


def main():
    ap = argparse.ArgumentParser(
        description="Process already-downloaded USLNDA WARC files into parquet."
    )
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--month", type=int, required=True)
    ap.add_argument("--site_csv", required=True)
    ap.add_argument("--download_root", default="warc_cache")
    ap.add_argument("--output_root", default="article_parquet")
    ap.add_argument("--summary_csv", default="results/month_summary.csv")
    ap.add_argument("--buffer_limit", type=int, default=200)
    ap.add_argument("--report_every_records", type=int, default=2500)
    ap.add_argument("--report_every_html", type=int, default=500)
    ap.add_argument("--workers", type=int, default=4, help="Parallel WARC workers inside one identifier")
    ap.add_argument("--start_identifier", default=None)
    ap.add_argument("--end_identifier", default=None)
    ap.add_argument("--max_identifiers", type=int, default=None)
    args = ap.parse_args()

    log("============================================================")
    log("USLNDA processing pipeline starting")
    log(f"year={args.year}")
    log(f"month={args.month:02d}")
    log(f"site_csv={args.site_csv}")
    log(f"download_root={args.download_root}")
    log(f"output_root={args.output_root}")
    log(f"summary_csv={args.summary_csv}")
    log(f"buffer_limit={args.buffer_limit}")
    log(f"report_every_records={args.report_every_records}")
    log(f"report_every_html={args.report_every_html}")
    log(f"workers={args.workers}")
    log(f"start_identifier={args.start_identifier}")
    log(f"end_identifier={args.end_identifier}")
    log(f"max_identifiers={args.max_identifiers}")
    log(f"cwd={os.getcwd()}")
    log("============================================================")

    state_to_host_map = load_site_mapping(args.site_csv)

    identifiers = list(
        iter_identifiers_for_month(
            args.year,
            args.month,
            start_identifier=args.start_identifier,
            end_identifier=args.end_identifier,
        )
    )
    if args.max_identifiers is not None:
        identifiers = identifiers[: args.max_identifiers]

    for identifier in identifiers:
        process_identifier(
            identifier=identifier,
            download_root=args.download_root,
            state_to_host_map=state_to_host_map,
            output_root=args.output_root,
            summary_csv_path=args.summary_csv,
            buffer_limit=args.buffer_limit,
            report_every_records=args.report_every_records,
            report_every_html=args.report_every_html,
            workers=args.workers,
        )

    log("============================================================")
    log("USLNDA processing pipeline complete")
    log("============================================================")


if __name__ == "__main__":
    main()