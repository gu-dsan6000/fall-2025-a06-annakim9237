#!/usr/bin/env python3

import os
import sys
import time
import logging
from pathlib import Path
from pyspark.sql.functions import (
    regexp_extract, col, rand, to_timestamp, trim, input_file_name
)
from pyspark.sql import DataFrame, SparkSession

# Default input path (local sample on the master node)
#input_path = "file:///home/ubuntu/spark-cluster/data/sample/"
# For full dataset on S3 (manual switch if you want)
input_path = "s3a://hk1105-assignment-spark-cluster-logs/data/"

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
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully for cluster execution")
    return spark

# -------------------- This is for local--------------------
# def local_input_glob() -> str:
#     """Return file:// glob pointing to data/sample/application_*/**"""
#     abs_sample = os.path.abspath("data/sample").rstrip("/")
#     return f"file:///{abs_sample}/application_*/**"

# def local_out_dir() -> Path:
#     """Outputs go to repo-local data/output/"""
#     d = Path("data/output")
#     d.mkdir(parents=True, exist_ok=True)
#     return d

# -------------------- Read --------------------
# def read_raw_local(spark: SparkSession) -> DataFrame:
#     read_path = local_input_glob()
#     print("=" * 70)
#     print("LOCAL TEST — using professor's regex")
#     print(f"Reading from: {read_path}")
#     print("=" * 70)
#     return spark.read.text(read_path).withColumn("file_path", input_file_name())

def read_raw(spark: SparkSession, base_path: str) -> DataFrame:
    read_path = _to_glob(base_path)
    print("=" * 70)
    print("READING LOGS")
    print(f"Reading from: {read_path}")
    print("=" * 70)
    return spark.read.text(read_path).withColumn("file_path", input_file_name())

# -------------------- Parse (Code from ReadMe that Professor given) --------------------
def parse_logs_with_ids(logs_df: DataFrame) -> DataFrame:
    logs_parsed = logs_df.select(
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1).alias('log_level'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)\s+([^:]+):', 2).alias('component'),
        col('value').alias('message'),
        col('file_path')
    )
    df = logs_parsed.withColumn(
        'application_id', regexp_extract('file_path', r'application_(\d+_\d+)', 0)
    ).withColumn(
        'container_id', regexp_extract('file_path', r'(container_\d+_\d+_\d+_\d+)', 1)
    )
    df = df.withColumn('ts', to_timestamp('timestamp', 'yy/MM/dd HH:mm:ss'))
    return df


def write_counts(parsed_df: DataFrame, out_dir: Path) -> Path:
    """data/output/problem1_counts.csv"""
    has_level = parsed_df.where(trim(col("log_level")) != "")
    out_path = out_dir / "problem1_counts.csv"
    (has_level.groupBy("log_level")
              .count()
              .orderBy("log_level")
              .toPandas()
              .to_csv(out_path, index=False))
    print(f"[OK] Wrote {out_path}")
    return out_path

def write_sample(parsed_df: DataFrame, out_dir: Path, n: int = 10) -> Path:
    """data/output/problem1_sample.csv"""
    has_level = parsed_df.where(trim(col("log_level")) != "")
    out_path = out_dir / "problem1_sample.csv"
    (has_level.orderBy(rand())
              .limit(n)
              .select("message", "log_level")
              .toPandas()
              .to_csv(out_path, index=False))
    print(f"[OK] Wrote {out_path}")
    return out_path

def write_summary(raw: DataFrame, parsed: DataFrame, out_dir: Path) -> Path:
    total_lines = raw.count()
    has_level = parsed.where(trim(col("log_level")) != "")
    lines_with_levels = has_level.count()
    counts_pd = (has_level.groupBy("log_level").count().orderBy("log_level")).toPandas()
    unique_levels = 0 if counts_pd.empty else counts_pd["log_level"].nunique()

    total_for_pct = max(lines_with_levels, 1)
    pct_lines = []
    for _, r in counts_pd.iterrows():
        pct = 100.0 * r["count"] / total_for_pct
        pct_lines.append(f"  {r['log_level']:<5}: {r['count']:>10,} ({pct:6.2f}%)")

    out_path = out_dir / "problem1_summary.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"Total log lines processed: {total_lines:,}\n")
        f.write(f"Total lines with log levels: {lines_with_levels:,}\n")
        f.write(f"Unique log levels found: {unique_levels}\n\n")
        f.write("Log level distribution:\n")
        f.write("\n".join(pct_lines) + "\n")
    print(f"[OK] Wrote {out_path}")
    return out_path

# -------------------- Trouble shooting --------------------
def _to_glob(path: str) -> str:
    p = path.rstrip("/")
    return p if ("application_*" in p or p.endswith("/**")) else f"{p}/application_*/**"

def _resolve_out_dir(input_path: str) -> Path:
    if input_path.startswith("file://"):
        out = Path("data/output")
    elif input_path.startswith("s3a://"):
        out = Path.home() / "spark-cluster"
    else:
        out = Path("data/output")
    out.mkdir(parents=True, exist_ok=True)
    return out




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
        if env_bucket and not net_id:
            bucket_a = env_bucket.replace("s3://", "s3a://", 1)
            path = f"{bucket_a}/data/"
            logger.info(f"SPARK_LOGS_BUCKET detected; overriding path to: {path}")

    try:
        logger.info("Problem 1 — building outputs with professor regex")
        #raw = read_raw_local(spark)
        raw = read_raw(spark, path)
        parsed = parse_logs_with_ids(raw)

        out_dir = _resolve_out_dir(path)

        write_counts(parsed, out_dir)
        write_sample(parsed, out_dir, n=10)
        write_summary(raw, parsed, out_dir)
        success = True
        logger.info("Problem 1 outputs created successfully")
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
    print("✅ PROBLEM 1 COMPLETED SUCCESSFULLY! " if success else "❌ Problem 1 failed")
    if success:
        print("(This script only prints; it does not write output files yet.)")
        print(f"  {out_dir / 'problem1_counts.csv'}")
        print(f"  {out_dir / 'problem1_sample.csv'}")
        print(f"  {out_dir / 'problem1_summary.txt'}")
    print("=" * 70)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
