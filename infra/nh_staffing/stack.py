"""NhStaffingStack: every AWS resource for the nursing home staffing pipeline.

"""
from pathlib import Path

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_athena as athena
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3d
from aws_cdk import aws_secretsmanager as sm
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subs
from constructs import Construct

REPO = Path(__file__).resolve().parents[2]    # infra/nh_staffing/stack.py -> the repo root


class NhStaffingStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.folder_ids = self.node.try_get_context("driveFolderIds")
        self.alert_email = self.node.try_get_context("alertEmail")
        if not self.folder_ids or not self.alert_email or "<" in f"{self.folder_ids}{self.alert_email}":
            raise ValueError("Fill in driveFolderIds and alertEmail in infra/cdk.json (Phase 4a, step 3)")

        self._foundation()      # Phase 4a: bucket, script upload, secret, alert topic, databases, workgroup
        # self._ingest()        # Phase 4b: the ingest job and its role
        # self._bronze()        # Phase 4c: the Spark role and the six bronze jobs
        # self._silver()        # Phase 4d: four silver jobs
        # self._gold()          # Phase 4e: five gold jobs
        # self._workflow()      # Phase 4f: workflow, schedule and triggers
        # self._alerts()        # Phase 4f: failure emails

    # ---- Phase 4a ---------------------------------------------------------------------------
    def _foundation(self):
        self.lake = s3.Bucket(
            self, "Lake",
            bucket_name=f"nh-staffing-lake-{self.account}",
            encryption=s3.BucketEncryption.S3_MANAGED,              # SSE-S3: no KMS key to manage
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,                   # cdk destroy removes it ...
            auto_delete_objects=True,                               # ... after emptying it
            lifecycle_rules=[
                s3.LifecycleRule(id="athena-results-7-days", prefix="athena-results/",
                                 expiration=Duration.days(7)),
                # quarantine/ has no rule: an expired copy would make bronze retry the bad file
                s3.LifecycleRule(id="old-versions-30-days", noncurrent_version_expiration=Duration.days(30),
                                 abort_incomplete_multipart_upload_after=Duration.days(1)),
            ],
        )

        # Job code, the registry, SQL and rulesets go to scripts/<folder>/ on every deploy.
        # One deployment per folder, so removing a file from one folder never touches another.
        for folder in ["src/glue", "config", "sql", "dq"]:
            name = folder.split("/")[-1]
            s3d.BucketDeployment(
                self, f"Deploy-{name}",
                sources=[s3d.Source.asset(str(REPO / folder))],
                destination_bucket=self.lake,
                destination_key_prefix=f"scripts/{name}/",
            )

        self.gdrive_secret = sm.Secret(
            self, "GdriveKey", secret_name="nh/gdrive/service-account",
            description="Google service account JSON key for nh-drive-reader (stored in Phase 4b)",
        )

        self.alerts = sns.Topic(self, "Alerts", topic_name="nh-pipeline-alerts")
        self.alerts.add_subscription(subs.EmailSubscription(self.alert_email))

        for db in ["nh_bronze", "nh_silver", "nh_gold"]:
            glue.CfnDatabase(self, db, catalog_id=self.account,
                             database_input=glue.CfnDatabase.DatabaseInputProperty(name=db))

        athena.CfnWorkGroup(
            self, "Workgroup", name="nh-wg", recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                bytes_scanned_cutoff_per_query=1_000_000_000,      # stop any query that scans 1 GB
                engine_version=athena.CfnWorkGroup.EngineVersionProperty(
                    selected_engine_version="Athena engine version 3"),
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{self.lake.bucket_name}/athena-results/"),
            ),
        )

        CfnOutput(self, "LakeBucket", value=self.lake.bucket_name)
        CfnOutput(self, "AlertTopicArn", value=self.alerts.topic_arn)