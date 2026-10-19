"""Check that the Google key in Secrets Manager can list both Drive folders.

Reads the key from AWS Secrets Manager and the folder IDs from infra/cdk.json, then lists each folder
the way the ingest job will.
"""
import json
from pathlib import Path

import boto3
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

key = json.loads(boto3.client("secretsmanager").get_secret_value(SecretId="nh/gdrive/service-account")["SecretString"])
creds = service_account.Credentials.from_service_account_info(
    key, scopes=["https://www.googleapis.com/auth/drive.readonly"])
sess = AuthorizedSession(creds)
print("service account:", key["client_email"])
for folder_id in json.loads(Path("infra/cdk.json").read_text())["context"]["driveFolderIds"]:
    r = sess.get("https://www.googleapis.com/drive/v3/files", timeout=60, params={
        "q": f"'{folder_id}' in parents and trashed = false", "pageSize": 1000,
        "fields": "files(name, mimeType, md5Checksum, size)"})
    r.raise_for_status()
    files = r.json()["files"]
    print(f"\nfolder {folder_id}: {len(files)} files")
    for f in sorted(files, key=lambda f: f["name"]):
        print(f"  {f['name']:<80} {f.get('md5Checksum', 'NO MD5: ' + f['mimeType'])}")