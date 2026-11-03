"""AWS Glue job: Divvy raw CSV (S3) -> curated Parquet (S3) + quarantine.

Deployed by infra/template.yaml. The transformation logic lives in
src/divvy_lakehouse/transform.py and is shipped to Glue as a zip via
--extra-py-files, so the code tested locally is the code that runs in AWS.

Job arguments (set in the CloudFormation template):
  --RAW_PATH         s3://<raw-bucket>/divvy_trips/
  --CURATED_PATH     s3://<curated-bucket>/divvy_trips/
  --QUARANTINE_PATH  s3://<curated-bucket>/divvy_trips_quarantine/
  --METRICS_PATH     s3://<curated-bucket>/_run_metrics/
"""

import json
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from divvy_lakehouse.transform import read_raw, run_metrics, transform, write_parquet

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "RAW_PATH", "CURATED_PATH", "QUARANTINE_PATH", "METRICS_PATH"]
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
# Only overwrite the year/month partitions this run actually produces,
# instead of wiping the whole curated table.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

raw = read_raw(spark, args["RAW_PATH"])
curated, quarantine = transform(raw)
curated.cache()
quarantine.cache()

write_parquet(curated, args["CURATED_PATH"], partitioned=True)
write_parquet(quarantine, args["QUARANTINE_PATH"], partitioned=False)

metrics = run_metrics(raw, curated, quarantine)
metrics["job_run_finished_utc"] = datetime.now(timezone.utc).isoformat()
bucket, _, prefix = args["METRICS_PATH"][5:].partition("/")
boto3.client("s3").put_object(
    Bucket=bucket,
    Key=f"{prefix.rstrip('/')}/run_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.json",
    Body=json.dumps(metrics, indent=2).encode(),
)
print(json.dumps(metrics, indent=2))

job.commit()
