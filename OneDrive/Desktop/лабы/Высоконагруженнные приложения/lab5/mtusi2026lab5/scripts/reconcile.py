#!/usr/bin/env python3
"""Reconciler for finding orphan/missing objects between S3 and PostgreSQL."""
import os
import boto3
import psycopg2
from botocore.config import Config
from datetime import datetime, timedelta

DB = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    user="postgres",
    password="secret",
    dbname="messenger"
)
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id=os.getenv("S3_ACCESS_KEY", "rustfsadmin"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY", "rustfssecret"),
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)
BUCKET = "attachments"
GRACE_HOURS = 0  # Set to 0 for demo purposes. In production, use 1 hour.

def reconcile(dry_run=True):
    cur = DB.cursor()
    cur.execute("SELECT id, s3_key, created_at FROM attachments")
    db_files = {row[1]: (row[0], row[2]) for row in cur.fetchall()}

    response = s3.list_objects_v2(Bucket=BUCKET)
    s3_files = {obj['Key']: obj['LastModified'] for obj in response.get('Contents', [])}

    print(f"Files in DB: {len(db_files)}")
    print(f"Files in S3: {len(s3_files)}")

    # Orphans: in S3 but not in DB
    orphans = set(s3_files.keys()) - set(db_files.keys())
    for key in orphans:
        age = datetime.now(s3_files[key].tzinfo) - s3_files[key]
        print(f"[ORPHAN] {key} (age: {age})")
        if age > timedelta(hours=GRACE_HOURS):
            if not dry_run:
                s3.delete_object(Bucket=BUCKET, Key=key)
                print(f"  -> Deleted from S3")
        else:
            print(f"  -> Skipped (fresh, age < {GRACE_HOURS}h)")

    # Missing: in DB but not in S3
    missing = set(db_files.keys()) - set(s3_files.keys())
    for key in missing:
        print(f"[MISSING] {key} exists in DB, but not in S3 (ERROR!)")

    if dry_run:
        print("\nDRY-RUN. To actually delete, run with --fix")

if __name__ == "__main__":
    import sys
    reconcile(dry_run="--fix" not in sys.argv)