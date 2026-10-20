"""Helpers shared by the bronze, silver and gold jobs (passed to each with --extra-py-files)."""
import re

import boto3
import yaml


def read_text(bucket, key):
    """A text file deployed under scripts/: the registry, a SQL transform or a DQDL ruleset."""
    return boto3.client("s3").get_object(Bucket=bucket, Key=f"scripts/{key}")["Body"].read().decode("utf-8")


def load_registry(bucket):
    return yaml.safe_load(read_text(bucket, "config/datasets.yml"))["datasets"]


def list_keys(bucket, prefix):
    keys = []
    for page in boto3.client("s3").get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def snake(name):
    """'Number of Certified Beds' -> number_of_certified_beds. Delta rejects spaces and ,;{}()= in names."""
    return re.sub(r"[^0-9a-z]+", "_", name.lower()).strip("_")


MONTHS = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()


def release_from_name(key):
    """CMS release a file belongs to, from its name: Oct2024 or 20241027 -> 2024-10, 2024_04 -> 2024-04,
    Q2_2024 -> 2024-q2, FY_2024 -> fy2024. Silver uses it to keep the newest release of each row."""
    name = key.rsplit("/", 1)[-1].split("__")[0]
    m = re.search(r"FY_(\d{4})", name)
    if m:
        return f"fy{m[1]}"
    m = re.search(r"Q([1-4])_(\d{4})", name)
    if m:
        return f"{m[2]}-q{m[1]}"
    m = re.search(r"([A-Z][a-z]{2})(\d{4})", name)
    if m and m[1] in MONTHS:
        return f"{m[2]}-{MONTHS.index(m[1]) + 1:02d}"
    m = re.search(r"(\d{4})_?(\d{2})(\d{2})?$", name)
    if m:
        return f"{m[1]}-{m[2]}"
    return None

# Silver entities: each bronze source feeds one entity through its SQL transform.
# key: one row per key in the silver table. dqdl: the ruleset every incoming row is checked against.
ENTITIES = {
    "staffing_daily": {
        "sources": {"pbj_daily_staffing": "sql/silver/staffing_daily.sql"},
        "key": ["ccn", "work_date"], "partition": ["work_month"],
        "dqdl": "dq/silver_staffing_daily.dqdl"},
    "facility": {
        "sources": {"provider_info": "sql/silver/facility.sql"},
        "key": ["ccn"], "dqdl": "dq/silver_facility.dqdl"},
    "state_benchmarks": {
        "sources": {"state_us_averages": "sql/silver/state_benchmarks.sql"},
        "key": ["state"], "dqdl": "dq/silver_state_benchmarks.dqdl"},
    "facility_quality": {
        "sources": {"quality_claims": "sql/silver/fq_claims.sql",
                    "snf_qrp": "sql/silver/fq_qrp.sql",
                    "snf_vbp": "sql/silver/fq_vbp.sql"},
        "key": ["ccn", "source", "measure_code"], "dqdl": "dq/silver_facility_quality.dqdl"},
}