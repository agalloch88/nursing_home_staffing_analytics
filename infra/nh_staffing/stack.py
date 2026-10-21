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
        self._ingest()          # Phase 4b: the ingest job and its role
        self._bronze()          # Phase 4c: the Spark role and the six bronze jobs
        self._silver()          # Phase 4d: four silver jobs
        self._gold()            # Phase 4e: five gold jobs
        self._workflow()        # Phase 4f: workflow, schedule and triggers
        self._alerts()          # Phase 4f: failure emails

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
    
    # ---- Phase 4b ---------------------------------------------------------------------------
    def _ingest(self):
        # The only role that can read the Google key. It reads the job code and the registry and
        # writes landing/ only; the workflow permissions let it record and stop its own run (4f).
        self.ingest_role = iam.Role(
            self, "IngestJobRole", role_name="nh-ingest-job",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")],
        )
        self.gdrive_secret.grant_read(self.ingest_role)
        self.lake.grant_read(self.ingest_role, "scripts/*")
        self.lake.grant_read_write(self.ingest_role, "landing/*")      # includes aborting multipart uploads
        self.ingest_role.add_to_policy(iam.PolicyStatement(
            actions=["glue:GetWorkflowRunProperties", "glue:PutWorkflowRunProperties", "glue:StopWorkflowRun"],
            resources=[f"arn:aws:glue:{self.region}:{self.account}:workflow/nh-pipeline"],
        ))

        self.ingest_job = glue.CfnJob(
            self, "IngestJob", name="nh_ingest_gdrive",
            role=self.ingest_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell", python_version="3.9",
                script_location=f"s3://{self.lake.bucket_name}/scripts/glue/ingest_gdrive.py"),
            max_capacity=0.0625,                  # 1/16 DPU: 1 GB of memory, enough to stream 8 MB parts
            timeout=30, max_retries=1,            # a rerun is safe: files already landed are skipped
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=1),
            default_arguments={
                "library-set": "analytics",       # preinstalled boto3 and requests
                "--additional-python-modules": "google-auth,pyyaml",
                "--bucket": self.lake.bucket_name,
                "--registry_key": "scripts/config/datasets.yml",
                "--secret_id": "nh/gdrive/service-account",
                "--folder_ids": ",".join(self.folder_ids),
            },
        )

    # ---- Phase 4c ---------------------------------------------------------------------------
    def _spark_job(self, name, script, extra_args):
        """One Glue 5.1 Spark job. The 15 bronze, silver and gold jobs differ only in these three things."""
        conf = " --conf ".join([                  # Glue takes several Spark settings through one --conf
            "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
            "spark.hadoop.fs.s3a.endpoint.region=us-east-1",     # Glue 5.1's S3 connector assumes us-east-2
        ])
        return glue.CfnJob(
            self, name, name=name, role=self.spark_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl", python_version="3",
                script_location=f"s3://{self.lake.bucket_name}/scripts/glue/{script}"),
            glue_version="5.1", worker_type="G.1X", number_of_workers=2,
            timeout=20, max_retries=0,            # a retry would repeat the same failure
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=1),
            default_arguments={
                "--datalake-formats": "delta",
                "--conf": conf,
                "--extra-py-files": f"s3://{self.lake.bucket_name}/scripts/glue/common.py",
                "--additional-python-modules": "pyyaml==6.0.2",   # Glue 5.1 doesn't ship PyYAML
                "--bucket": self.lake.bucket_name,
                "--enable-metrics": "true",
                **extra_args,
            },
        )

    def _bronze(self):
        # One role for all 15 Spark jobs: read landing/ and scripts/, write the zones they own.
        self.spark_role = iam.Role(
            self, "SparkJobsRole", role_name="nh-spark-jobs",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")],
        )
        self.lake.grant_read(self.spark_role, "landing/*")
        self.lake.grant_read(self.spark_role, "scripts/*")
        for prefix in ["bronze/*", "silver/*", "gold/*", "quarantine/*", "_checkpoints/*"]:
            self.lake.grant_read_write(self.spark_role, prefix)

        datasets = ["pbj_daily_staffing", "provider_info", "state_us_averages",
                    "quality_claims", "snf_qrp", "snf_vbp"]
        self.bronze_jobs = [self._spark_job(f"nh_bronze_{d}", "bronze.py", {"--dataset": d}) for d in datasets]

    # ---- Phase 4d ---------------------------------------------------------------------------
    def _silver(self):
        entities = ["staffing_daily", "facility", "state_benchmarks", "facility_quality"]
        self.silver_jobs = [self._spark_job(f"nh_silver_{e}", "silver.py", {"--entity": e}) for e in entities]

    # ---- Phase 4e ---------------------------------------------------------------------------
    def _gold(self):
        metrics = ["m1_staffing_hprd", "m2_nurse_hours", "m3_contract_share",
                   "m4_occupancy", "m5_benchmarks_outcomes"]
        # --exclude_suspect_days true leaves days above 24 HPRD and days with residents but no nurse
        # hours out of the metrics; false counts them, as CMS does.
        self.gold_jobs = [self._spark_job(f"nh_gold_{m}", "gold.py",
                                          {"--metric": m, "--exclude_suspect_days": "true"})
                          for m in metrics]

    # ---- Phase 4f ---------------------------------------------------------------------------
    def _workflow(self):
        # One run at a time: a start while a run is going is refused, so two runs never load the same files.
        workflow = glue.CfnWorkflow(self, "Workflow", name="nh-pipeline", max_concurrent_runs=1,
                                    description="Drive to landing, bronze, silver and gold")

        def trigger(name, kind, start, after=(), **kwargs):
            """Start the jobs in `start`; a CONDITIONAL trigger waits until every job in `after` succeeded.
            .ref is the workflow or job name, and also makes CloudFormation create those first."""
            predicate = glue.CfnTrigger.PredicateProperty(logical="AND", conditions=[
                glue.CfnTrigger.ConditionProperty(job_name=j.ref, state="SUCCEEDED", logical_operator="EQUALS")
                for j in after]) if after else None
            return glue.CfnTrigger(self, name, name=name, type=kind, workflow_name=workflow.ref,
                                   actions=[glue.CfnTrigger.ActionProperty(job_name=j.ref) for j in start],
                                   predicate=predicate, **kwargs)

        # The schedule is in UTC and is created switched off; Phase 4g switches it on.
        trigger("nh-daily", "SCHEDULED", [self.ingest_job], schedule="cron(0 11 * * ? *)",
                start_on_creation=True)
        trigger("nh-after-ingest", "CONDITIONAL", self.bronze_jobs, after=[self.ingest_job], start_on_creation=True)
        trigger("nh-after-bronze", "CONDITIONAL", self.silver_jobs, after=self.bronze_jobs, start_on_creation=True)
        trigger("nh-after-silver", "CONDITIONAL", self.gold_jobs, after=self.silver_jobs, start_on_creation=True)

    def _alerts(self):
        # Glue sends a "Glue Job State Change" event whenever a job run ends. Failures and timeouts of the
        # pipeline's jobs (names starting nh_) become emails; a run stopped because Drive had nothing new
        # ends STOPPED and stays quiet.
        rule = events.Rule(
            self, "GlueJobFailed", rule_name="nh-glue-job-failed",
            description="Email when an nh_ Glue job fails or times out",
            event_pattern=events.EventPattern(
                source=["aws.glue"], detail_type=["Glue Job State Change"],
                detail={"state": ["FAILED", "TIMEOUT"], "jobName": events.Match.prefix("nh_")}),
        )
        field = events.EventField.from_path
        rule.add_target(targets.SnsTopic(self.alerts, message=events.RuleTargetInput.from_text(
            f"Glue job {field('$.detail.jobName')} ended {field('$.detail.state')} at {events.EventField.time}. "
            f"Job run {field('$.detail.jobRunId')}. {field('$.detail.message')}")))