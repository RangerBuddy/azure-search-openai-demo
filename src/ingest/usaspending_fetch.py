#!/usr/bin/env python3
"""
Fetch awards for a recipient via USAspending API v2 and write per-page JSON to ADLS Gen2.
Usage: python src/ingest/usaspending_fetch.py --recipient-id <RECIPIENT_UUID> [--pagesize 50]
"""
import os, json, time, argparse, logging, requests
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def adls_client(account_name: str):
    cred = DefaultAzureCredential()
    url = f"https://{account_name}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=url, credential=cred)

def write_json_to_adls(svc, filesystem, path_dir, filename, data):
    fs = svc.get_file_system_client(filesystem)
    # ensure directory exists (safe to call)
    dir_client = fs.get_directory_client(path_dir)
    try:
        dir_client.create_directory()
    except Exception:
        pass
    # write file (overwrite)
    file_client = dir_client.get_file_client(filename)
    file_client.create_file()
    file_client.append_data(json.dumps(data, indent=2).encode("utf-8"), offset=0)
    file_client.flush_data(len(json.dumps(data).encode("utf-8")))

def fetch_usaspending_for_recipient(recipient_id, pagesize=50, sleep_s=0.2):
    base = "https://api.usaspending.gov/api/v2/recipient/awards/"
    # --- NEW: parse '-C' or '-P' suffix into separate param ---
    rid = recipient_id
    rlevel = None
    if rid.endswith("-C") or rid.endswith("-P"):
        rlevel = rid[-1]        # 'C' or 'P'
        rid = rid[:-2]          # strip the dash and suffix
    # If no suffix provided, default to Child ('C'); adjust if you need Parent
    if rlevel is None:
        rlevel = "C"
    params = {"recipient_id": rid, "recipient_level": rlevel, "limit": pagesize, "page": 1}
    # -----------------------------------------------------------
    session = requests.Session()
    session.headers.update({"User-Agent": "anduril-corpus-bot/0.1 (you@example.com)"})
    svc = adls_client(os.environ["AZURE_STORAGE_ACCOUNT_NAME"])
    fs = os.environ.get("ADLS_FILESYSTEM", "raw")
    prefix_dir = "anduril/USAspending"


    while True:
        logging.info("GET %s page=%s", base, params["page"])
        r = session.get(base, params=params, timeout=30)
        if r.status_code != 200:
            logging.error("USAspending returned %s: %s", r.status_code, r.text[:300])
            break
        payload = r.json()
        # write page JSON
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        filename = f"usaspending_{recipient_id}_page{params['page']}_{ts}.json"
        write_json_to_adls(svc, fs, f"{prefix_dir}/{recipient_id}", filename, payload)
        logging.info("Wrote page %s (hits=%s)", params["page"], len(payload.get("results",[])))
        # pagination judgement: usa_spending provides 'page_metadata' or 'next' etc.
        meta = payload.get("page_metadata") or payload.get("offset") or {}
        # simple stop condition: no results / empty results
        if not payload.get("results"):
            break
        params["page"] += 1
        time.sleep(sleep_s)
    logging.info("Done fetching USAspending for %s", recipient_id)

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--recipient-id", required=True)
    p.add_argument("--pagesize", type=int, default=50)
    args=p.parse_args()
    fetch_usaspending_for_recipient(args.recipient_id, pagesize=args.pagesize)
