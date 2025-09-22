#!/usr/bin/env python3
"""
Fetch RSS or HTML for permissive news sites and store snapshots to ADLS.
Usage: python src/ingest/news_fetch.py --url https://www.defensenews.com/feed/
"""
import os, argparse, logging, feedparser, requests
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
import json, time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def adls_client(account_name: str):
    cred = DefaultAzureCredential()
    url = f"https://{account_name}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=url, credential=cred)

def write_text(svc, filesystem, path_dir, filename, text):
    fs = svc.get_file_system_client(filesystem)
    dir_client = fs.get_directory_client(path_dir)
    try:
        dir_client.create_directory()
    except Exception:
        pass
    file_client = dir_client.get_file_client(filename)
    file_client.create_file()
    b = text.encode("utf-8")
    file_client.append_data(b, offset=0)
    file_client.flush_data(len(b))

def fetch_rss_and_store(url, source_name, max_items=10):
    svc = adls_client(os.environ["AZURE_STORAGE_ACCOUNT_NAME"])
    fs = os.environ.get("ADLS_FILESYSTEM","raw")
    prefix = f"anduril/news/{source_name}"
    logging.info("Parsing RSS: %s", url)
    feed = feedparser.parse(url)
    for entry in feed.entries[:max_items]:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        safe_title = entry.get("title", "untitled").replace("/","_").replace(" ","_")[:140]
        filename = f"{safe_title}_{ts}.json"
        item = {
            "title": entry.get("title"),
            "link": entry.get("link"),
            "published": entry.get("published"),
            "summary": entry.get("summary"),
        }
        # write feed item metadata
        write_text(svc, fs, f"{prefix}/rss", filename, json.dumps(item, indent=2))
        # optionally fetch full article HTML (if link allowed)
        try:
            r = requests.get(entry.link, timeout=15, headers={"User-Agent":"anduril-corpus-bot/0.1"})
            if r.status_code==200 and len(r.text)>200:
                html_name = f"{safe_title}_{ts}.html"
                write_text(svc, fs, f"{prefix}/html", html_name, r.text)
        except Exception as e:
            logging.warning("Failed to fetch article %s: %s", entry.link, e)
        time.sleep(0.2)

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--url", required=True)
    p.add_argument("--source", required=True)
    p.add_argument("--max", type=int, default=10)
    args=p.parse_args()
    fetch_rss_and_store(args.url, args.source, max_items=args.max)
