"""Write a workflow run's graph as a Mermaid diagram in Markdown, which GitHub renders as a picture.

Usage (from the repo root, after source scripts/env.sh):
  python scripts/run_graph.py <run id> > docs/incremental/drop1_graph.md
  python scripts/run_graph.py > docs/incremental/drop2_graph.md       (the latest run)
Each job shows how it ended and how long it ran: green succeeded, red failed, grey stopped,
dashed not started. Triggers are the hexagons between the stages.
"""
import datetime
import sys

import boto3

WORKFLOW = "nh-pipeline"
STYLE = {"SUCCEEDED": "ok", "FAILED": "failed", "TIMEOUT": "failed", "ERROR": "failed", "STOPPED": "stopped"}
glue = boto3.client("glue")

if len(sys.argv) > 1:
    run_id = sys.argv[1]
else:
    runs = glue.get_workflow_runs(Name=WORKFLOW, IncludeGraph=False, MaxResults=25)["Runs"]
    run_id = max(runs, key=lambda r: r["StartedOn"])["WorkflowRunId"]
run = glue.get_workflow_run(Name=WORKFLOW, RunId=run_id, IncludeGraph=True)["Run"]
graph = run.get("Graph", {})

ids = {}                                        # Glue node IDs are long; Mermaid gets n0, n1, ...
lines = ["flowchart LR"]
for n in sorted(graph.get("Nodes", []), key=lambda n: n["Name"]):
    key = ids.setdefault(n["UniqueId"], f"n{len(ids)}")
    if n["Type"] == "TRIGGER":
        lines.append(f'  {key}{{{{"{n["Name"]}"}}}}:::trigger')
        continue
    job_runs = n.get("JobDetails", {}).get("JobRuns", [])
    if job_runs:
        last = max(job_runs, key=lambda r: r["StartedOn"])
        label, style = f'{last["JobRunState"]}, {last.get("ExecutionTime", 0)} s', STYLE.get(last["JobRunState"], "stopped")
    else:
        label, style = "not started", "notrun"
    lines.append(f'  {key}["{n["Name"]}<br/>{label}"]:::{style}')
for e in graph.get("Edges", []):
    lines.append(f'  {ids[e["SourceId"]]} --> {ids[e["DestinationId"]]}')
lines += ["  classDef ok fill:#d8f0dc,stroke:#2e7d32,color:#1b3d1f",
          "  classDef failed fill:#f9d6d5,stroke:#c62828,color:#5a1414",
          "  classDef stopped fill:#e6e6e6,stroke:#757575,color:#333333",
          "  classDef notrun fill:#ffffff,stroke:#9e9e9e,stroke-dasharray:4 3,color:#555555",
          "  classDef trigger fill:#e8eefc,stroke:#3f5fa8,color:#1c2c55"]

props = " ".join(f"{k}={v}" for k, v in sorted(run.get("WorkflowRunProperties", {}).items())) or "none"
started = run["StartedOn"].astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
print(f"# {WORKFLOW} run {run_id}\n")
print(f"Started {started}; status {run['Status']}; run properties: {props}"
      + (f"; resumed from {run['PreviousRunId']}" if run.get("PreviousRunId") else "") + ".\n")
print("```mermaid\n" + "\n".join(lines) + "\n```")