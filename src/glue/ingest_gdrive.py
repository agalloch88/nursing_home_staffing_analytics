"""nh_ingest_gdrive: copy new source files from Google Drive to S3 landing.

Runs as a Glue Python shell job (Python 3.9). For every file in the configured Drive folders it
finds the one registry dataset whose pattern matches the name, and copies the file to
  landing/<dataset>/<yyyy-mm-dd>/<name>__<drive file id>__<md5>.csv
unless a landing key for that dataset already ends in the file's MD5. The landing keys are the
manifest: there is no other state. A failed copy is aborted, so it leaves nothing behind.

Inside the nh-pipeline workflow it records what it copied in the run properties and, when nothing
is new, stops the run so no Spark job starts (start the run with force=true to override).
"""
import datetime
import hashlib
import json
import re
import sys
import time

import boto3
import yaml
from awsglue.utils import getResolvedOptions
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

DRIVE = "https://www.googleapis.com/drive/v3/files"
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
PART = 8 * 1024 * 1024                  # S3 multipart parts must be at least 5 MB, except the last

in_workflow = "--WORKFLOW_RUN_ID" in sys.argv          # absent when you start the job by hand
args = getResolvedOptions(sys.argv, ["bucket", "registry_key", "secret_id", "folder_ids"]
                          + (["WORKFLOW_NAME", "WORKFLOW_RUN_ID"] if in_workflow else []))
s3, glue = boto3.client("s3"), boto3.client("glue")
bucket = args["bucket"]
registry = yaml.safe_load(s3.get_object(Bucket=bucket, Key=args["registry_key"])["Body"].read())["datasets"]


def drive_session():
    raw = boto3.client("secretsmanager").get_secret_value(SecretId=args["secret_id"])["SecretString"]
    creds = service_account.Credentials.from_service_account_info(json.loads(raw), scopes=SCOPES)
    return AuthorizedSession(creds)


def get(sess, url, **kw):
    """GET with backoff on Drive's rate-limit (429) and server (5xx) errors."""
    for attempt in range(6):
        r = sess.get(url, timeout=60, **kw)
        if r.status_code not in (429, 500, 502, 503, 504):
            r.raise_for_status()
            return r
        r.close()
        time.sleep(2 ** attempt)
    r.raise_for_status()


def list_folder(sess, folder_id):
    params = {"q": f"'{folder_id}' in parents and trashed = false", "pageSize": 1000,
              "fields": "nextPageToken, files(id, name, mimeType, md5Checksum, size)"}
    while True:
        body = get(sess, DRIVE, params=params).json()
        yield from body.get("files", [])
        if "nextPageToken" not in body:
            return
        params["pageToken"] = body["nextPageToken"]


def landed_md5s(dataset):
    """MD5s already in landing for a dataset: the last __-separated part of each key."""
    md5s = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"landing/{dataset}/"):
        for obj in page.get("Contents", []):
            md5s.add(obj["Key"].rsplit("__", 1)[-1].replace(".csv", ""))
    return md5s


def copy(sess, f, dataset):
    """Stream one Drive file into S3 in 8 MB parts, checking its MD5 before completing the upload."""
    stem = f["name"].rsplit(".", 1)[0]
    key = f"landing/{dataset}/{datetime.date.today():%Y-%m-%d}/{stem}__{f['id']}__{f['md5Checksum']}.csv"
    md5, parts, buf = hashlib.md5(), [], bytearray()
    upload_id = s3.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]

    def flush():
        n = len(parts) + 1
        etag = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=n, Body=bytes(buf))["ETag"]
        parts.append({"PartNumber": n, "ETag": etag})
        buf.clear()

    try:
        with get(sess, f"{DRIVE}/{f['id']}", params={"alt": "media"}, stream=True) as r:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                md5.update(chunk)
                buf.extend(chunk)
                if len(buf) >= PART:
                    flush()
        if buf or not parts:
            flush()
        if md5.hexdigest() != f["md5Checksum"]:
            raise ValueError(f"checksum mismatch for {f['name']}: Drive says {f['md5Checksum']}, "
                             f"downloaded {md5.hexdigest()}")
        s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts})
    except Exception:
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise                           # the job fails and alerts; the next run tries the file again
    print(f"copied {f['name']} ({int(f.get('size', 0)):,} bytes) to {key}")
    return key


sess = drive_session()
copied, skipped, rejected, seen = [], [], [], {}
for folder_id in args["folder_ids"].split(","):
    for f in list_folder(sess, folder_id):
        if f["mimeType"].startswith("application/vnd.google-apps"):
            rejected.append(f"{f['name']}: a Google Docs file, folder or shortcut, not an uploaded file")
            continue
        matches = [d for d, spec in registry.items() if re.match(spec["pattern"], f["name"])]
        if len(matches) != 1:           # none (the PDF data dictionary) or more than one
            rejected.append(f"{f['name']}: matches registry datasets {matches}")
            continue
        dataset = matches[0]
        if int(f.get("size", 0)) == 0:
            rejected.append(f"{f['name']}: empty file")
            continue
        if dataset not in seen:
            seen[dataset] = landed_md5s(dataset)
        if f["md5Checksum"] in seen[dataset]:
            skipped.append(f["name"])   # already landed, under any name or Drive file ID
            continue
        copied.append(copy(sess, f, dataset))
        seen[dataset].add(f["md5Checksum"])

print(json.dumps({"copied": len(copied), "skipped": len(skipped), "rejected": rejected}, indent=1))

if in_workflow:
    name, run_id = args["WORKFLOW_NAME"], args["WORKFLOW_RUN_ID"]
    props = glue.get_workflow_run_properties(Name=name, RunId=run_id)["RunProperties"]
    glue.put_workflow_run_properties(Name=name, RunId=run_id, RunProperties={
        "copied_files": str(len(copied)), "rejected_files": str(len(rejected))})
    if not copied and props.get("force") != "true":
        print("Nothing new in Drive: stopping the workflow run, so no Spark job starts.", flush=True)
        glue.stop_workflow_run(Name=name, RunId=run_id)     # also stops this job; the run ends Stopped
        time.sleep(120)