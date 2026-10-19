"""CDK entry point. cdk synth, cdk diff and cdk deploy run this file from infra/."""
import os

import aws_cdk as cdk

from nh_staffing.stack import NhStaffingStack

app = cdk.App()
stack = NhStaffingStack(
    app, "NhStaffingStack",
    env=cdk.Environment(account=os.environ.get("CDK_DEFAULT_ACCOUNT"), region="us-east-1"),
    description="Nursing home staffing pipeline: Glue, Delta Lake on S3, Athena",
)
cdk.Tags.of(stack).add("project", "nh-staffing")     # lets Cost Explorer and Budgets filter on it
app.synth()