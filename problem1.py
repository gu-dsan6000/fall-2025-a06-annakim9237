#!/usr/bin/env python3

import os
import sys
import time
import logging
from pyspark.sql import SparkSession

# Default input path (local sample on the master node)
input_path = "file:///home/ubuntu/spark-cluster/data/sample/"
# For full dataset on S3 (manual switch if you want)
# input_path = "s3a://hk1105-assignment-spark-cluster-logs/data/"

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)
logger = logging.getLogger(__name__)


def create_spark_session(master_url: str | None):
    """
    Create a SparkSession.
    If master_url is provided, bind to that cluster; otherwise, use Spark's default.
    """
    builder = (
        SparkSession.builder
        .appName("Problem1-Min-Check")
        # S3 (s3a://) configuration using IAM instance profile
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider",
        )
        .config("spark.sql.adaptive.enabled", "true")
    )
    if master_url:
        builder = builder.master(master_url)
    spark = builder.getOrCreate()
    logger.info("Spark session created successfully for cluster execution")
    return spark


def solve_problem1(spark, path: str):
    """
    Minimal 'peek' for Problem 1:
    - Read text logs (recursively) from `path`
    - Show first 10 lines
    - Print total line count
    """
    logger.info("Starting Problem 1 (peek)")
    print("=" * 70)
    print("PROBLEM 1: Log Level Distribution (peek)")
    print("=" * 70)

    logger.info(f"Reading text files from: {path}")
    print("Reading all text files into a single DataFrame...")
    df = (
        spark.read
        .option("recursiveFileLookup", "true")   # read nested files under application_* dirs
        .text(path)
    )  # single column: 'value'

    print("\n=== First 10 rows ===")
    df.show(10, truncate=False)
    print("=== End of 10 rows ===\n")

    total_count = df.count()
    logger.info(f"Total records (lines) in DataFrame: {total_count}")
    print(f"Total records (lines) in DataFrame: {total_count}")
    return df


def main():
    logger.info("Starting Problem 1")
    print("=" * 70)
    print("PROBLEM 1: Log Level Distribution")
    print("=" * 70)

    # 1) master_url: first positional arg if present; else use env MASTER_PRIVATE_IP
    if len(sys.argv) > 1 and not sys.argv[1].startswith("--"):
        master_url = sys.argv[1]
    else:
        master_private_ip = os.getenv("MASTER_PRIVATE_IP")
        if master_private_ip:
            master_url = f"spark://{master_private_ip}:7077"
        else:
            print("❌ Error: Master URL not provided")
            print("Run: python ~/problem1.py spark://$MASTER_PRIVATE_IP:7077 [INPUT_PATH] [--net-id YOUR_ID]")
            print("  or: export MASTER_PRIVATE_IP=xxx.xxx.xxx.xxx")
            return 1

    print(f"Connecting to Spark Master at: {master_url}")
    logger.info(f"Using Spark master URL: {master_url}")

    t0 = time.time()
    logger.info("Initializing Spark session for cluster execution")
    spark = create_spark_session(master_url)

    # 2) Resolve input path with this priority:
    #    (a) explicit second positional arg (highest priority)
    #    (b) --net-id <id>  -> s3a://<id>-assignment-spark-cluster-logs/data/
    #    (c) SPARK_LOGS_BUCKET env -> s3a://<bucket>/data/
    #    (d) file-level default `input_path`
    explicit_path = False
    if len(sys.argv) > 2 and not sys.argv[2].startswith("--"):
        path = sys.argv[2]
        explicit_path = True
        logger.info(f"Using explicit input path from argv[2]: {path}")
    else:
        path = input_path  # default (will be overridden by --net-id/env if provided)

    # Parse --net-id only if path was not explicitly provided
    if not explicit_path:
        argv_tail = sys.argv[1:]
        net_id = None
        for i, a in enumerate(argv_tail):
            if a == "--net-id" and i + 1 < len(argv_tail):
                net_id = argv_tail[i + 1]
                break
        if net_id:
            bucket = f"s3a://{net_id}-assignment-spark-cluster-logs"
            path = f"{bucket}/data/"
            logger.info(f"--net-id detected; using path: {path}")

        # Env override only if path was not explicitly provided
        env_bucket = os.getenv("SPARK_LOGS_BUCKET")
        if env_bucket:
            bucket_a = env_bucket.replace("s3://", "s3a://", 1)
            path = f"{bucket_a}/data/"
            logger.info(f"SPARK_LOGS_BUCKET detected; overriding path to: {path}")

    try:
        logger.info("Starting Problem 1 analysis (peek mode)")
        _ = solve_problem1(spark, path)
        success = True
        logger.info("Problem 1 analysis completed successfully")
    except Exception as e:
        logger.exception(f"Error occurred while solving Problem 1: {e}")
        print(f"❌ Error solving Problem 1: {e}")
        success = False
    finally:
        spark.stop()

    elapsed = time.time() - t0
    logger.info(f"Total execution time: {elapsed:.2f} seconds")
    print(f"Total execution time: {elapsed:.2f} seconds")

    print("\n" + "=" * 70)
    print("✅ PROBLEM 1 COMPLETED SUCCESSFULLY! (peek mode)" if success else "❌ Problem 1 failed")
    if success:
        print("(This script only prints; it does not write output files yet.)")
    print("=" * 70)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
