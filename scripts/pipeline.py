"""Start, resume or inspect a run of the nh-pipeline Glue workflow, and print how every job ended.

Usage (from the repo root, after source scripts/env.sh):
  python scripts/pipeline.py start            start a run, wait for it, print the result
  python scripts/pipeline.py start --force    the same, but the Spark jobs run even if Drive has nothing new
  python scripts/pipeline.py resume           rerun the stage that failed in the latest run, and everything after it
                                              (or: resume <run id>)
  python scripts/pipeline.py status           print the latest run (or: status <run id>)
Exits with status 1 if any job failed or timed out.
"""
import sys
import time

import boto3

WORKFLOW = "nh-pipeline"
FAILED = {"FAILED", "TIMEOUT", "ERROR"}
STAGES = ["nh_ingest", "nh_bronze", "nh_silver", "nh_gold"]
glue = boto3.client("glue")


def get_run(run_id):
    return glue.get_workflow_run(Name=WORKFLOW, RunId=run_id, IncludeGraph=True)["Run"]


def latest_run_id():
    runs = glue.get_workflow_runs(Name=WORKFLOW, IncludeGraph=False, MaxResults=25)["Runs"]
    if not runs:
        sys.exit(f"{WORKFLOW} has no runs yet")
    return max(runs, key=lambda r: r["StartedOn"])["WorkflowRunId"]


def stage(job_name):
    return next((i for i, s in enumerate(STAGES) if job_name.startswith(s)), len(STAGES))


def job_nodes(run):
    """(job name, its latest run in this workflow run or None, node ID), in pipeline order."""
    nodes = []
    for n in run.get("Graph", {}).get("Nodes", []):
        if n["Type"] == "JOB":
            runs = n.get("JobDetails", {}).get("JobRuns", [])
            nodes.append((n["Name"], max(runs, key=lambda r: r["StartedOn"]) if runs else None, n["UniqueId"]))
    return sorted(nodes, key=lambda n: (stage(n[0]), n[0]))


def wait(run_id):
    last = None
    while True:
        run = glue.get_workflow_run(Name=WORKFLOW, RunId=run_id, IncludeGraph=False)["Run"]
        s = run.get("Statistics", {})
        line = (f"{run['Status']:9} succeeded {s.get('SucceededActions', 0)}, running {s.get('RunningActions', 0)}, "
                f"failed {s.get('FailedActions', 0) + s.get('TimeoutActions', 0) + s.get('ErroredActions', 0)}, "
                f"stopped {s.get('StoppedActions', 0)}")
        if line != last:
            print(f"{time.strftime('%H:%M:%S')}  {line}", flush=True)
            last = line
        if run["Status"] in ("COMPLETED", "STOPPED", "ERROR"):
            return
        time.sleep(30)


def report(run_id):
    run = get_run(run_id)
    nodes = job_nodes(run)
    failed = [name for name, r, _ in nodes if r and r["JobRunState"] in FAILED]
    print(f"\n{WORKFLOW} run {run_id}: {run['Status']}")
    if run.get("PreviousRunId"):
        print(f"resumed from {run['PreviousRunId']}")
    props = run.get("WorkflowRunProperties", {})
    print("run properties: " + (" ".join(f"{k}={v}" for k, v in sorted(props.items())) or "none"))
    for name, r, _ in nodes:
        if r is None:
            print(f"  {name:34} not started")
            continue
        print(f"  {name:34} {r['JobRunState']:10} {r.get('ExecutionTime', 0):5} s")
        if r.get("ErrorMessage"):
            print(f"      {r['ErrorMessage'][:600]}")
    started = sum(1 for _, r, _ in nodes if r)
    print(f"{started} of {len(nodes)} jobs started, {len(failed)} failed")
    if run["Status"] == "COMPLETED" and started == 0:
        print("No job started: the start trigger is probably off. Check it with\n"
              "  aws glue get-trigger --name nh-daily --query Trigger.State      (should be ACTIVATED)\n"
              "and switch it on with: aws glue start-trigger --name nh-daily")
    return failed


def main():
    command = sys.argv[1] if len(sys.argv) > 1 else "status"
    if command == "start":
        props = {"force": "true"} if "--force" in sys.argv else {}
        run_id = glue.start_workflow_run(Name=WORKFLOW, RunProperties=props)["RunId"]
        print(f"started {WORKFLOW} run {run_id}" + (" with force=true" if props else ""))
        wait(run_id)
    elif command == "resume":
        previous = sys.argv[2] if len(sys.argv) > 2 else latest_run_id()
        nodes = job_nodes(get_run(previous))
        failed_stages = {stage(name) for name, r, _ in nodes if r and r["JobRunState"] in FAILED}
        if not failed_stages:
            sys.exit(f"run {previous} has no failed jobs to resume")
        # Rerun every job of the failed stage, not only the failed one: the next trigger waits for all of
        # them to succeed, and AWS doesn't document whether successes copied from the original run count.
        # Rerunning a job that already succeeded is harmless; every job skips what it has already done.
        node_ids = [node_id for name, r, node_id in nodes if r and stage(name) in failed_stages]
        run_id = glue.resume_workflow_run(Name=WORKFLOW, RunId=previous, NodeIds=node_ids)["RunId"]
        print(f"resumed {previous} from the {len(node_ids)} job(s) of the failed stage as run {run_id}")
        wait(run_id)
    elif command == "status":
        run_id = sys.argv[2] if len(sys.argv) > 2 else latest_run_id()
    else:
        sys.exit(__doc__)
    sys.exit(1 if report(run_id) else 0)


if __name__ == "__main__":
    main()