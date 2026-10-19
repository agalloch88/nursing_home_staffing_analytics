"""nh_bronze_<dataset>: append new landing files to bronze/<dataset>/ as a Delta table.

One script, six jobs: --dataset picks the registry entry. Every column stays text; silver casts.
For each landing file not yet in the table or in quarantine, it decodes the whole file strictly,
checks the header against the registry, and parses every row. A file that fails any check is
copied to quarantine/ with an .error.json beside it. Good files are appended in one Delta commit,
then the job fails if anything was quarantined, so the run alerts.
"""
import codecs
import csv
import json
import sys

import boto3
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import list_keys, load_registry, release_from_name, snake

args = getResolvedOptions(sys.argv, ["dataset", "bucket"])
ds, bucket = args["dataset"], args["bucket"]
spec = load_registry(bucket)[ds]
spark, s3 = SparkSession.builder.getOrCreate(), boto3.client("s3")
table = f"s3://{bucket}/bronze/{ds}/"

loaded = set()
if DeltaTable.isDeltaTable(spark, table):
    loaded = {r[0] for r in spark.read.format("delta").load(table).select("_source_file").distinct().collect()}
in_quarantine = {"landing/" + k[len("quarantine/"):] for k in list_keys(bucket, f"quarantine/{ds}/")}
new = sorted(k for k in list_keys(bucket, f"landing/{ds}/") if k not in loaded and k not in in_quarantine)
print(f"{ds}: {len(loaded)} files already in bronze, {len(new)} new")


def pick_encoding(key):
    """The first registry encoding that decodes the whole file without an error. Without this check,
    Spark reads Windows-1252 bytes as UTF-8 without complaint and stores garbled names."""
    for enc in spec["encoding"]:
        dec = codecs.getincrementaldecoder(enc)(errors="strict")
        try:
            for chunk in s3.get_object(Bucket=bucket, Key=key)["Body"].iter_chunks(8 * 1024 * 1024):
                dec.decode(chunk)
            dec.decode(b"", final=True)
            return enc
        except UnicodeDecodeError:
            continue
    raise ValueError(f"not decodable as any of {spec['encoding']}")


def check_header(key, enc):
    head = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-65535")["Body"].read()
    header = next(csv.reader([head.decode(enc, errors="ignore").lstrip("﻿").splitlines()[0]]))
    if header != spec["columns"]:
        raise ValueError(f"header differs from the registry: {header}")


def quarantine(key, err):
    target = "quarantine/" + key[len("landing/"):]
    s3.copy_object(Bucket=bucket, Key=target, CopySource={"Bucket": bucket, "Key": key})
    s3.put_object(Bucket=bucket, Key=target.replace(".csv", ".error.json"),
                  Body=json.dumps({"source": key, "error": str(err)}))
    print(f"QUARANTINED {key}: {err}")


schema = T.StructType([T.StructField(c, T.StringType()) for c in spec["columns"]])
rename = spec.get("rename", {})
frames, failed = [], []
for key in new:
    try:
        enc = pick_encoding(key)
        check_header(key, enc)
    except Exception as err:
        quarantine(key, err)
        failed.append(key)
        continue
    df = (spark.read.option("header", True).option("mode", "FAILFAST").option("escape", '"')
          .option("encoding", enc).schema(schema).csv(f"s3://{bucket}/{key}")).cache()
    try:
        rows = df.count()                       # with cache(), count() parses every column of every row
        if rows == 0:
            raise ValueError("no data rows")
    except Exception as err:                    # a ragged row fails here, for this file only
        df.unpersist()
        quarantine(key, str(err)[:2000])
        failed.append(key)
        continue
    print(f"{key}: {rows:,} rows, {enc}")
    _, file_id, md5 = key.rsplit("__", 2)
    frames.append(df.toDF(*[rename.get(c, snake(c)) for c in spec["columns"]])
                  .withColumn("_source_file", F.lit(key))
                  .withColumn("_drive_file_id", F.lit(file_id))
                  .withColumn("_md5", F.lit(md5.replace(".csv", "")))
                  .withColumn("_release", F.lit(release_from_name(key)).cast("string"))
                  .withColumn("_ingested_at", F.current_timestamp()))

if frames:                                      # one Delta commit for every new file
    out = frames[0]
    for df in frames[1:]:
        out = out.unionByName(df)
    out.write.format("delta").mode("append").save(table)
print(json.dumps({"dataset": ds, "appended": [k for k in new if k not in failed], "quarantined": failed}))
if failed:
    raise RuntimeError(f"{len(failed)} file(s) quarantined: {failed}")