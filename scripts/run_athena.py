"""Run each statement in a SQL file in Athena's nh-wg workgroup, in order, and print any results.

${bucket} in the file becomes nh-staffing-lake-<account ID>. Stops at the first failure.
Usage (from the repo root, after source scripts/env.sh):
  python scripts/run_athena.py sql/athena/tables_bronze.sql
"""
import re
import sys
import time

import boto3

athena = boto3.client("athena")
bucket = f"nh-staffing-lake-{boto3.client('sts').get_caller_identity()['Account']}"
text = open(sys.argv[1]).read().replace("${bucket}", bucket)
body = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--"))
statements = [s.strip() for s in re.split(r";[ \t]*(?:\n|$)", body) if s.strip()]

for sql in statements:
    qid = athena.start_query_execution(QueryString=sql, WorkGroup="nh-wg")["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    print(f"\n{status['State']}: {' '.join(sql.split())[:100]}")
    if status["State"] != "SUCCEEDED":
        print(status.get("StateChangeReason", ""))
        sys.exit(1)
    rows = athena.get_query_results(QueryExecutionId=qid, MaxResults=100)["ResultSet"]["Rows"]
    table = [[c.get("VarCharValue", "") for c in r["Data"]] for r in rows]
    if table:
        widths = [max(len(r[i]) for r in table) for i in range(len(table[0]))]
        for r in table:
            print("  " + "  ".join(v.ljust(w) for v, w in zip(r, widths)))