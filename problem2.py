#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time
from pathlib import Path
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    regexp_extract, col, to_timestamp, min as smin, max as smax, count as scount
)
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# -------------------- Spark (local only) --------------------
def create_spark_local() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("Problem2-Local-Timeline")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# -------------------- Paths --------------------
def local_input_glob() -> str:
    abs_sample = os.path.abspath("data/sample").rstrip("/")
    return f"file:///{abs_sample}/application_*/**"

def out_dir() -> Path:
    d = Path("data/output")
    d.mkdir(parents=True, exist_ok=True)
    return d

# -------------------- Read & Parse (same from problem1) --------------------
def read_raw_local(spark: SparkSession) -> DataFrame:
    read_path = local_input_glob()
    print("=" * 70)
    print("PROBLEM 2 — LOCAL SAMPLE")
    print(f"Reading from: {read_path}")
    print("=" * 70)
    return spark.read.text(read_path).withColumn("file_path", input_file_name())

from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import trim

def parse_logs_with_ids(logs_df: DataFrame) -> DataFrame:
    parsed = logs_df.select(
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1).alias('log_level'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)\s+([^:]+):', 2).alias('component'),
        col('value').alias('message'),
        col('file_path')
    )
    parsed = parsed.withColumn(
        'application_id', regexp_extract('file_path', r'application_(\d+_\d+)', 0)
    ).withColumn(
        'container_id', regexp_extract('file_path', r'(container_\d+_\d+_\d+_\d+)', 1)
    )
    parsed = parsed.withColumn(
        'cluster_id', regexp_extract('application_id', r'application_(\d+)_\d+', 1)
    )
    parsed = parsed.withColumn(
        'app_number', regexp_extract('application_id', r'application_\d+_(\d+)', 1)
    )
    parsed = parsed.withColumn('ts', to_timestamp('timestamp', 'yy/MM/dd HH:mm:ss'))
    return parsed

# -------------------- Build timeline & summary --------------------
def build_timeline(parsed: DataFrame) -> DataFrame:

    df = parsed.where(col("ts").isNotNull() & (trim(col("application_id")) != ""))
    grouped = (df.groupBy("cluster_id", "application_id", "app_number")
                 .agg(smin("ts").alias("start_time"),
                      smax("ts").alias("end_time")))

    return grouped


# -------------------- Main --------------------
def main() -> int:
    t0 = time.time()
    spark = create_spark_local()
    try:
        raw = read_raw_local(spark)
        parsed = parse_logs_with_ids(raw)

        timeline_sdf = build_timeline(parsed)

        out = out_dir()



        print(f"\nElapsed: {time.time()-t0:.2f}s")
        print("=" * 70)
        print("✅ LOCAL STEP DONE — Problem 2 outputs generated in data/output/")
        print("=" * 70)
        return 0
    finally:
        spark.stop()

if __name__ == "__main__":
    raise SystemExit(main())
