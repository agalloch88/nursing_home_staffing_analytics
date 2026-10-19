"""Start Glue jobs, wait for them to finish, and print how each ended with the end of its log.

Usage (from the repo root, after source scripts/env.sh):
  python scripts/run_jobs.py nh_ingest_gdrive
  python scripts/run_jobs.py nh_bronze_provider_info nh_bronze_state_us_averages
Exits with status 1 if any job didn't succeed.
"""
import sys
import time

import boto3

DONE = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR", "EXPIRED"}
# Where print() output lands: Python shell jobs log to a stream named after the run ID, Glue 5 Spark
# jobs to <run ID>-driver.
LOGS = [("/aws-glue/python-jobs/output", ""), ("/aws-glue/jobs/output", "-driver")]

glue, logs = boto3.client("glue"), boto3.client("logs")
runs = {}
for job in sys.argv[1:]:
    runs[job] = glue.start_job_run(JobName=job)["JobRunId"]
    print(f"started {job} {runs[job]}")

states = {}
while len(states) < len(runs):
    time.sleep(15)
    for job, run_id in runs.items():
        if job not in states:
            run = glue.get_job_run(JobName=job, RunId=run_id)["JobRun"]
            if run["JobRunState"] in DONE:
                states[job] = run
                print(f"{job}: {run['JobRunState']} after {run.get('ExecutionTime', 0)} s")

for job, run in states.items():
    print(f"\n==== {job} {run['Id']}: {run['JobRunState']}")
    if run.get("ErrorMessage"):
        print("error:", run["ErrorMessage"])
    for group, suffix in LOGS:
        stream = run["Id"] + suffix
        try:
            events = logs.get_log_events(logGroupName=group, logStreamName=stream, limit=25,
                                         startFromHead=False)["events"]
        except logs.exceptions.ResourceNotFoundException:
            continue
        print(f"-- last lines of {group} {stream}")
        for e in events:
            print(e["message"].rstrip())
        break

sys.exit(0 if all(r["JobRunState"] == "SUCCEEDED" for r in states.values()) else 1)