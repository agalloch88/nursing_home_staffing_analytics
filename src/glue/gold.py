"""nh_gold_<metric>: rebuild one gold table from silver and replace it in one Delta commit.

One script, five jobs: --metric names sql/gold/<metric>.sql. The four silver tables are registered as
views, the metric's SQL builds the whole table, and an overwrite replaces it atomically, so a reader
sees either the old table or the new one. If dq/gold_<metric>.dqdl exists, its rules run afterwards as
warnings: published to the Data quality tab and printed, never failing the job.
"""
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import SelectFromCollection
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.context import SparkContext

from common import list_keys, read_text

args = getResolvedOptions(sys.argv, ["metric", "bucket", "exclude_suspect_days"])
metric, bucket, exclude = args["metric"], args["bucket"], args["exclude_suspect_days"].lower()
if exclude not in ("true", "false"):
    raise ValueError("--exclude_suspect_days must be true or false")
glue_ctx = GlueContext(SparkContext.getOrCreate())
spark = glue_ctx.spark_session

for table in ["staffing_daily", "facility", "state_benchmarks", "facility_quality"]:
    spark.read.format("delta").load(f"s3://{bucket}/silver/{table}/").createOrReplaceTempView(table)

sql = read_text(bucket, f"sql/gold/{metric}.sql").replace("${exclude_suspect_days}", exclude)
df = spark.sql(sql).cache()
(df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
   .save(f"s3://{bucket}/gold/{metric}/"))
print(f"gold {metric}: {df.count():,} rows written (exclude_suspect_days={exclude})")

if list_keys(bucket, f"scripts/dq/gold_{metric}.dqdl"):
    results = EvaluateDataQuality().process_rows(
        frame=DynamicFrame.fromDF(df, glue_ctx, metric), ruleset=read_text(bucket, f"dq/gold_{metric}.dqdl"),
        publishing_options={"dataQualityEvaluationContext": f"gold_{metric}",
                            "enableDataQualityCloudWatchMetrics": True,
                            "enableDataQualityResultsPublishing": True})
    for r in SelectFromCollection.apply(dfc=results, key="ruleOutcomes").toDF().collect():
        label = "DQ" if r["Outcome"] == "Passed" else "DQ WARNING"
        print(f"{label} gold_{metric}: {r['Outcome']} {r['Rule']} {r['FailureReason'] or ''}")