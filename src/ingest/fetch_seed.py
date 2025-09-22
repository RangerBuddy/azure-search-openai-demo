#!/usr/bin/env python3
"""
src/ingest/fetch_seed.py

Minimal polite seeder:
 - Reads data/anduril_seed_sources.csv
 - For each row, checks robots.txt
 - Calls APIs (USAspending / SAM) when api_endpoint_or_params present
 - Fetches HTML/PDF otherwise
 - Writes raw file + metadata sidecar to ADLS Gen2 (preferred) or local fallback

Dependencies:
  pip install requests azure-identity azure-storage-file-datalake python-dateutil
"""

import csv
import hashlib
import io
import json
import os
import random
import sys
import time
import logging
from datetime import datetime
from urllib.parse import urlparse, urljoin

import requests
from dateutil import tz
from urllib import robotparser

# Azure SDK imports (optional - used if AZURE_STORAGE_ACCOUNT_NAME is set)
try:
    from azure.identity import DefaultAzureCredential
    from azure.storage.filedatalake import DataLakeServiceClient
    AZURE_SDK_AVAILABLE = True
except Exception:
    AZURE_SDK_AVAILABLE = False

LOG = logging.getLogger("ingest")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Configurable defaults
CSV_PATH = os.environ.get("SEED_CSV_PATH", "data/anduril_seed_sources.csv")
LOCAL_RAW_BASE = os.environ.get("LOCAL_RAW_BASE", "data/raw/anduril")
DEFAULT_CRAWL_DELAY = float(os.environ.get("DEFAULT_CRAWL_DELAY", "5.0"))
USER_AGENT = os.environ.get("CRAWLER_USER_AGENT", "anduril-corpus-bot/0.1 (+mailto:you@yourorg.example)")
ADLS_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
ADLS_FILESYSTEM = os.environ.get("ADLS_FILESYSTEM")  # e.g., "raw"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate, br"})

# per-domain state
domain_last_request = {}
domain_crawl_delay = {}

# ADLS client helper
class ADLSWriter:
    def __init__(self, account_name: str, filesystem: str):
        if not AZURE_SDK_AVAILABLE:
            raise RuntimeError("Azure SDK (azure-identity, azure-storage-file-datalake) is required for ADLS writes.")
        account_url = f"https://{account_name}.dfs.core.windows.net"
        cred = DefaultAzureCredential()
        self.svc = DataLakeServiceClient(account_url=account_url, credential=cred)
        self.filesystem = filesystem
        # ensure filesystem exists
        try:
            self.svc.create_file_system(filesystem)
            LOG.info("Created filesystem %s", filesystem)
        except Exception:
            # exists or permission denied; proceed
            pass

    def upload_bytes(self, directory_path: str, filename: str, data: bytes):
        dir_client = self.svc.get_directory_client(self.filesystem, directory_path)
        try:
            dir_client.create_directory()
        except Exception:
            pass
        file_client = dir_client.create_file(filename)
        file_client.upload_data(data, overwrite=True)
        return True

    def upload_text(self, directory_path: str, filename: str, text: str, encoding="utf-8"):
        self.upload_bytes(directory_path, filename, text.encode(encoding))


def sha256_hex(b: bytes):
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def load_manifest(csv_path):
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(filter(lambda r: r.strip() and not r.strip().startswith("#"), fh))
        for row in reader:
            rows.append(row)
    return rows


def get_domain(url):
    try:
        p = urlparse(url)
        return p.netloc.lower()
    except Exception:
        return None


def get_robots_parser_for_domain(domain, scheme="https"):
    robots_url = f"https://{domain}/robots.txt"
    rp = robotparser.RobotFileParser()
    try:
        rp.set_url(robots_url)
        rp.read()
        return rp
    except Exception as e:
        LOG.warning("Failed to read robots.txt %s: %s", robots_url, e)
        return None


def wait_for_domain(domain):
    last = domain_last_request.get(domain)
    delay = domain_crawl_delay.get(domain, DEFAULT_CRAWL_DELAY)
    if last:
        elapsed = time.time() - last
        if elapsed < delay:
            sleep_for = delay - elapsed + random.uniform(0, 1)
            LOG.debug("Sleeping %.2fs to respect crawl-delay for %s", sleep_for, domain)
            time.sleep(sleep_for)


def record_domain_request(domain, robots_parser=None):
    # extract crawl-delay if exists and set domain_crawl_delay
    if robots_parser:
        try:
            cd = robots_parser.crawl_delay(USER_AGENT)
            if cd is None:
                cd = robots_parser.crawl_delay("*")
        except Exception:
            cd = None
        if cd is not None:
            try:
                domain_crawl_delay[domain] = float(cd)
            except Exception:
                domain_crawl_delay[domain] = DEFAULT_CRAWL_DELAY
    domain_last_request[domain] = time.time()


def polite_fetch(url, allow_fetch=True, stream=False, params=None, headers=None):
    domain = get_domain(url)
    rp = get_robots_parser_for_domain(domain) if domain else None
    if rp:
        can = rp.can_fetch(USER_AGENT, url)
        if not can:
            LOG.info("robots.txt disallows fetching %s for %s", url, USER_AGENT)
            return {"skipped_by_robots": True, "url": url}
    # enforce per-domain rate limiting
    wait_for_domain(domain)
    # perform request
    try:
        LOG.info("Fetching %s", url)
        resp = SESSION.get(url, params=params, headers=headers or {}, timeout=60, stream=stream)
        record_domain_request(domain, rp)
        return {"skipped_by_robots": False, "response": resp}
    except requests.RequestException as e:
        LOG.warning("Request failed for %s: %s", url, e)
        return {"skipped_by_robots": False, "error": str(e), "url": url}


def ensure_local_path(path):
    os.makedirs(path, exist_ok=True)


def save_local_bytes(target_path, filename, data: bytes):
    ensure_local_path(target_path)
    full = os.path.join(target_path, filename)
    with open(full, "wb") as fh:
        fh.write(data)
    return full


def save_local_text(target_path, filename, text: str):
    ensure_local_path(target_path)
    full = os.path.join(target_path, filename)
    with open(full, "w", encoding="utf-8") as fh:
        fh.write(text)
    return full


def build_output_paths(source_name):
    # normalized directory: use only alphanum + dash
    safe_name = "".join([c if c.isalnum() or c in "-_" else "_" for c in source_name]).strip("_-")
    date_part = datetime.utcnow().strftime("%Y%m%d")
    return f"anduril/{safe_name}/{date_part}"


def main(csv_path=CSV_PATH, dry_run=False, local_only=False):
    LOG.info("Loading manifest %s", csv_path)
    rows = load_manifest(csv_path)
    LOG.info("Found %d rows", len(rows))

    adls_writer = None
    if ADLS_ACCOUNT and ADLS_FILESYSTEM and AZURE_SDK_AVAILABLE and not local_only:
        try:
            adls_writer = ADLSWriter(ADLS_ACCOUNT, ADLS_FILESYSTEM)
            LOG.info("ADLS writer initialized for account=%s filesystem=%s", ADLS_ACCOUNT, ADLS_FILESYSTEM)
        except Exception as e:
            LOG.warning("Could not initialize ADLS writer: %s. Falling back to local writes.", e)
            adls_writer = None

    for row in rows:
        source_name = row.get("source_name") or row.get("source") or "unnamed"
        url = row.get("url") or ""
        fetch_method = (row.get("fetch_method") or "").lower()
        api_params = row.get("api_endpoint_or_params") or ""
        notes = row.get("notes") or ""
        manifest_meta = {k: v for k, v in row.items()}

        outdir = build_output_paths(source_name)
        # Determine filenames
        parsed = urlparse(url)
        basename = parsed.path.rstrip("/").split("/")[-1] or "index"
        if "." not in basename:
            # default extension based on content type later
            basename = basename + ".html"

        # API shortcuts
        if "usaspending" in url.lower() or "usaspending" in api_params.lower():
            # USAspending API: we'll use the public API root if needed
            api_base = "https://api.usaspending.gov/api/v2"
            # example: recipient awards or recipient profile - best effort: try recipient/awards with recipient_id in url/query
            try:
                # Try to extract recipient id from manifest if present in api_params
                params = {}
                if api_params:
                    # support simple "recipient_id=..." style
                    for p in api_params.split("&"):
                        if "=" in p:
                            k, v = p.split("=", 1)
                            params[k.strip()] = v.strip()
                if "recipient_id" in params or "recipient_unique_id" in params:
                    endpoint = f"{api_base}/recipient/awards/"
                    # USAspending supports POST search endpoints for complex queries; we'll do a GET if possible
                    LOG.info("Querying USAspending awards endpoint for params=%s", params)
                    if dry_run:
                        LOG.info("[dry-run] would GET %s with params %s", endpoint, params)
                        continue
                    resp = SESSION.get(endpoint, params=params, timeout=60)
                    content = resp.content
                    meta = {
                        "source_name": source_name,
                        "url": endpoint,
                        "fetched_at": datetime.utcnow().isoformat(),
                        "http_status": resp.status_code,
                        "notes": notes,
                    }
                    filename = f"{source_name}_usaspending_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
                    if adls_writer:
                        adls_writer.upload_bytes(outdir, filename, content)
                        adls_writer.upload_text(outdir, filename + ".metadata.json", json.dumps(meta, indent=2))
                        LOG.info("Wrote to ADLS: %s/%s", outdir, filename)
                    else:
                        local_dir = os.path.join(LOCAL_RAW_BASE, outdir)
                        save_local_bytes(local_dir, filename, content)
                        save_local_text(local_dir, filename + ".metadata.json", json.dumps(meta, indent=2))
                        LOG.info("Wrote local: %s/%s", local_dir, filename)
                    continue
            except Exception as e:
                LOG.warning("USAspending helper failed: %s", e)
                # fallback to generic fetch

        # SAM.gov API use if api params present and SAM_API_KEY env var set
        if "sam.gov" in url.lower() or ("sam" in api_params.lower() and os.environ.get("SAM_API_KEY")):
            sam_key = os.environ.get("SAM_API_KEY")
            if sam_key:
                # example: use /prod/opportunities/v2/search?api_key=...
                try:
                    endpoint = "https://api.sam.gov/prod/opportunities/v2/search"
                    params = {}
                    # if api_params looks like "query=Anduril", parse into params
                    for p in api_params.split("&"):
                        if "=" in p:
                            k, v = p.split("=", 1)
                            params[k.strip()] = v.strip()
                    params["api_key"] = sam_key
                    LOG.info("Querying SAM.gov opportunities: %s params=%s", endpoint, params)
                    if dry_run:
                        LOG.info("[dry-run] would GET %s with params %s", endpoint, params)
                        continue
                    resp = SESSION.get(endpoint, params=params, timeout=60)
                    content = resp.content
                    meta = {
                        "source_name": source_name,
                        "url": endpoint,
                        "fetched_at": datetime.utcnow().isoformat(),
                        "http_status": resp.status_code,
                        "notes": notes,
                    }
                    filename = f"{source_name}_sam_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
                    if adls_writer:
                        adls_writer.upload_bytes(outdir, filename, content)
                        adls_writer.upload_text(outdir, filename + ".metadata.json", json.dumps(meta, indent=2))
                        LOG.info("Wrote to ADLS: %s/%s", outdir, filename)
                    else:
                        local_dir = os.path.join(LOCAL_RAW_BASE, outdir)
                        save_local_bytes(local_dir, filename, content)
                        save_local_text(local_dir, filename + ".metadata.json", json.dumps(meta, indent=2))
                        LOG.info("Wrote local: %s/%s", local_dir, filename)
                    continue
                except Exception as e:
                    LOG.warning("SAM.gov helper failed: %s", e)
                    # fallback to generic fetch

        # Generic fetch path (HTML/PDF)
        if not url:
            LOG.warning("No URL for row %s - skipping", source_name)
            continue

        # Check robots and perform polite fetch
        rp = get_robots_parser_for_domain(get_domain(url))
        if rp and not rp.can_fetch(USER_AGENT, url):
            LOG.info("Robots disallow %s -> skipping", url)
            meta = {
                "source_name": source_name,
                "url": url,
                "fetched_at": datetime.utcnow().isoformat(),
                "skipped_by_robots": True,
                "notes": notes,
            }
            # write metadata sidecar only
            filename = f"{source_name}_robots_skip_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
            if adls_writer:
                adls_writer.upload_text(outdir, filename, json.dumps(meta, indent=2))
                LOG.info("Wrote ADLS metadata for robots skip: %s/%s", outdir, filename)
            else:
                local_dir = os.path.join(LOCAL_RAW_BASE, outdir)
                save_local_text(local_dir, filename, json.dumps(meta, indent=2))
                LOG.info("Wrote local metadata for robots skip: %s/%s", local_dir, filename)
            continue

        # fetch content
        if dry_run:
            LOG.info("[dry-run] would fetch %s", url)
            continue

        resp_info = polite_fetch(url)
        if resp_info.get("skipped_by_robots"):
            LOG.info("Skipped by robots: %s", url)
            continue
        if "error" in resp_info:
            LOG.warning("Fetch error for %s: %s", url, resp_info.get("error"))
            continue
        resp = resp_info.get("response")
        if not resp:
            LOG.warning("No response for %s", url)
            continue

        # read content (stream=False earlier)
        try:
            content = resp.content
        except Exception as e:
            LOG.warning("Failed to read content for %s: %s", url, e)
            continue

        # normalize filename extension using content-type
        content_type = resp.headers.get("Content-Type", "").lower()
        if "pdf" in content_type:
            ext = ".pdf"
        elif "html" in content_type or "text" in content_type:
            ext = ".html"
        else:
            ext = os.path.splitext(basename)[1] or ".bin"

        filename_ts = f"{basename.rstrip('.')}_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}{ext}"
        checksum = sha256_hex(content)
        meta = {
            "source_name": source_name,
            "url": url,
            "fetched_at": datetime.utcnow().isoformat(),
            "http_status": resp.status_code,
            "content_type": content_type,
            "content_length": len(content),
            "etag": resp.headers.get("ETag"),
            "checksum_sha256": checksum,
            "notes": notes,
            "manifest_row": manifest_meta,
        }

        if adls_writer:
            try:
                adls_writer.upload_bytes(outdir, filename_ts, content)
                adls_writer.upload_text(outdir, filename_ts + ".metadata.json", json.dumps(meta, indent=2))
                LOG.info("Wrote to ADLS: %s/%s", outdir, filename_ts)
            except Exception as e:
                LOG.warning("ADLS upload failed for %s: %s", url, e)
                # fallback to local
                local_dir = os.path.join(LOCAL_RAW_BASE, outdir)
                save_local_bytes(local_dir, filename_ts, content)
                save_local_text(local_dir, filename_ts + ".metadata.json", json.dumps(meta, indent=2))
                LOG.info("Wrote fallback local: %s/%s", local_dir, filename_ts)
        else:
            local_dir = os.path.join(LOCAL_RAW_BASE, outdir)
            save_local_bytes(local_dir, filename_ts, content)
            save_local_text(local_dir, filename_ts + ".metadata.json", json.dumps(meta, indent=2))
            LOG.info("Wrote local: %s/%s", local_dir, filename_ts)

        # minimal politeness: if we receive 429, back off
        if resp.status_code == 429:
            backoff = int(resp.headers.get("Retry-After", DEFAULT_CRAWL_DELAY))
            LOG.info("Received 429. Backing off %ds", backoff)
            time.sleep(backoff + random.uniform(0, 1))

    LOG.info("Done seeding manifest.")


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Seed ingestion: fetch manifest and write raw artifacts to ADLS or local path.")
    p.add_argument("--csv", default=CSV_PATH, help="Path to seed CSV manifest")
    p.add_argument("--dry-run", action="store_true", help="Do not fetch, just show plan")
    p.add_argument("--local-only", action="store_true", help="Force local writes only (no ADLS)")
    args = p.parse_args()
    main(csv_path=args.csv, dry_run=args.dry_run, local_only=args.local_only)
