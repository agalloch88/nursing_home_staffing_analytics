"""nh_silver_<entity>: typed, one-row-per-key silver tables, loaded incrementally.

One script, four jobs: --entity picks an entry in common.ENTITIES. For each bronze source of the
entity it starts a Delta stream with availableNow, so a run reads only the bronze rows added since
its last run (the checkpoint under _checkpoints/ remembers where it stopped) and then ends.
Each micro-batch is transformed by the source's SQL, checked against the entity's Glue Data Quality
ruleset, reduced to the newest row per key, and MERGEd into silver/<entity>/.
"""
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import SelectFromCollection
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from delta.tables import DeltaTable
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import ENTITIES, load_registry, read_text

args = getResolvedOptions(sys.argv, ["entity", "bucket"])
name, bucket = args["entity"], args["bucket"]
ent = ENTITIES[name]
glue_ctx = GlueContext(SparkContext.getOrCreate())
spark = glue_ctx.spark_session
target = f"s3://{bucket}/silver/{name}/"
ruleset = read_text(bucket, ent["dqdl"])
registry = load_registry(bucket)
skip = [k for source in ent["sources"] for k in registry[source].get("skip_files", [])]
newest_first = Window.partitionBy(*ent["key"]).orderBy(
    F.col("_release").desc_nulls_last(), F.col("_ingested_at").desc(), F.col("_source_file").desc())


def check_quality(df):
    """Evaluate the ruleset on every incoming row; fail the job, before anything is written, if a rule fails."""
    results = EvaluateDataQuality().process_rows(
        frame=DynamicFrame.fromDF(df, glue_ctx, name), ruleset=ruleset,
        publishing_options={"dataQualityEvaluationContext": f"silver_{name}",
                            "enableDataQualityCloudWatchMetrics": True,
                            "enableDataQualityResultsPublishing": True})
    outcomes = SelectFromCollection.apply(dfc=results, key="ruleOutcomes").toDF().collect()
    for r in outcomes:
        print(f"DQ silver_{name}: {r['Outcome']:<6} {r['Rule']}")
    failed = [r["Rule"] for r in outcomes if r["Outcome"] == "Failed"]
    if failed:
        raise RuntimeError(f"silver_{name}: data quality rules failed: {failed}")


def make_upsert(source, transform_sql):
    def upsert(batch, batch_id):
        batch = batch.filter(~F.col("_source_file").isin(skip))
        if batch.isEmpty():
            return
        batch.createOrReplaceTempView("batch")
        rows = batch.sparkSession.sql(transform_sql).cache()
        check_quality(rows)
        latest = (rows.withColumn("_rn", F.row_number().over(newest_first))
                  .filter("_rn = 1").drop("_rn"))          # MERGE needs one source row per key
        if not DeltaTable.isDeltaTable(spark, target):
            writer = latest.write.format("delta")
            if ent.get("partition"):
                writer = writer.partitionBy(*ent["partition"])
            writer.save(target)
        else:
            on = " and ".join(f"t.{k} = s.{k}" for k in ent["key"])
            (DeltaTable.forPath(spark, target).alias("t")
                .merge(latest.alias("s"), on)
                # update only rows that changed, and never with an older CMS release
                .whenMatchedUpdateAll(condition="t._row_hash <> s._row_hash and "
                                                "coalesce(s._release, '') >= coalesce(t._release, '')")
                .whenNotMatchedInsertAll()
                .execute())
        last = DeltaTable.forPath(spark, target).history(1).collect()[0]
        print(f"{name} <- {source} batch {batch_id}: {rows.count():,} rows in, "
              f"Delta v{last['version']} {last['operation']} {dict(last['operationMetrics'])}")
        rows.unpersist()
    return upsert


for source, sql_key in ent["sources"].items():         # one stream and one checkpoint per bronze source
    query = (spark.readStream.format("delta").load(f"s3://{bucket}/bronze/{source}/")
             .writeStream.foreachBatch(make_upsert(source, read_text(bucket, sql_key)))
             .option("checkpointLocation", f"s3://{bucket}/_checkpoints/silver/{name}/{source}/")
             .trigger(availableNow=True)                # process what is new, then stop
             .start())
    query.awaitTermination()
    print(f"{name} <- {source}: stream finished")