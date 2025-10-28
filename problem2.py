#!/usr/bin/env python3

import os
import sys
import time
import logging
from pathlib import Path
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    regexp_extract, col, to_timestamp, input_file_name, date_format
)

# -------------------- Defaults (LOCAL MODE) --------------------
LOCAL_SAMPLE_DIR = Path("data/sample")
input_path = str(LOCAL_SAMPLE_DIR)

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)
logger = logging.getLogger(__name__)


def create_spark_session(master_url: str | None):
    """Create a SparkSession. Defaults to local[*] if not given."""
    if not master_url:
        master_url = "local[*]"

    builder = (
        SparkSession.builder
        .master(master_url)
        .appName("Problem2-Timeline-Summary (Local)")
        .config("spark.sql.adaptive.enabled", "true")

        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider",
        )
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark session created successfully with master={master_url}")
    return spark


# -------------------- Path helpers --------------------
def _to_glob(path: str) -> str:
    p = path.rstrip("/")
    if p.startswith("s3a://"):
        return f"{p}/application_*/*.log"   
    abs_p = Path(p).resolve().as_posix().rstrip("/")
    return f"file:///{abs_p}/application_*/*.log"  


def _resolve_out_dir(_: str) -> Path:
    d = Path("data/output")
    d.mkdir(parents=True, exist_ok=True)
    return d


# -------------------- Read & Parse --------------------
def read_raw(spark: SparkSession, base_path: str) -> DataFrame:
    read_path = _to_glob(base_path)
    print("=" * 70)
    print("READING LOGS")
    print(f"Reading from: {read_path}")
    print("=" * 70)
    return spark.read.text(read_path).withColumn("file_path", input_file_name())

def parse_logs_with_ids(logs_df: DataFrame) -> DataFrame:
    ts_raw = F.regexp_extract('value', r'^\s*(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1)
    parsed = logs_df.select(
        ts_raw.alias('ts_raw'),
        F.regexp_extract('file_path', r'(application_\d+_\d+)', 1).alias('application_id'),
        'file_path'
    ).withColumn(
        'ts', F.expr("try_to_timestamp(ts_raw, 'yy/MM/dd HH:mm:ss')")
    ).withColumn(
        'cluster_id', F.regexp_extract('application_id', r'application_(\d+)_\d+', 1)
    ).withColumn(
        'app_number', F.regexp_extract('application_id', r'_(\d+)$', 1)
    ).where(
        (F.col("ts").isNotNull()) &
        (F.col("application_id") != "") &
        (F.col("cluster_id") != "")
    )
    return parsed

# -------------------- Problem 2 outputs --------------------
def build_application_timeline(parsed: DataFrame) -> DataFrame:
    """
    Expected output 1 (timeline):
    cluster_id,application_id,app_number,start_time,end_time
    """
    apps = (
        parsed.groupBy("application_id", "cluster_id", "app_number")
              .agg(F.min("ts").alias("start_ts"),
                   F.max("ts").alias("end_ts"))
    )

    apps = (
        apps
        .withColumn("start_time", date_format("start_ts", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("end_time",   date_format("end_ts",   "yyyy-MM-dd HH:mm:ss"))
        .select("cluster_id", "application_id", "app_number", "start_time", "end_time")
        .orderBy("cluster_id", F.col("app_number").cast("int"))
    )
    return apps


def build_cluster_summary(timeline_df: DataFrame) -> DataFrame:

    with_ts = (
        timeline_df
        .withColumn("start_ts", F.to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("end_ts",   F.to_timestamp("end_time",   "yyyy-MM-dd HH:mm:ss"))
    )

    summary = (
        with_ts.groupBy("cluster_id")
               .agg(
                   F.count("*").alias("num_applications"),
                   F.min("start_ts").alias("cluster_first_ts"),
                   F.max("end_ts").alias("cluster_last_ts"),
               )
               .withColumn("cluster_first_app", date_format("cluster_first_ts", "yyyy-MM-dd HH:mm:ss"))
               .withColumn("cluster_last_app",  date_format("cluster_last_ts",  "yyyy-MM-dd HH:mm:ss"))
               .select("cluster_id", "num_applications", "cluster_first_app", "cluster_last_app")
               .orderBy(F.col("num_applications").desc(), "cluster_id")
    )
    return summary

def save_bar_chart(cluster_df: DataFrame, out_dir: Path) -> Path:

    pdf = cluster_df.orderBy(F.col("num_applications").desc(), "cluster_id").toPandas()
    if pdf.empty:
        return out_dir / "problem2_bar_chart.png"  # nothing to draw

    x = pdf["cluster_id"].astype(str).tolist()
    y = pdf["num_applications"].astype(int).tolist()

    plt.figure(figsize=(max(8, len(x)*0.6), 5))
    # color map
    cmap = plt.get_cmap("tab20")
    colors = [cmap(i % 20) for i in range(len(x))]

    bars = plt.bar(x, y, color=colors)

    # value labels
    for b, v in zip(bars, y):
        plt.text(b.get_x() + b.get_width()/2, b.get_height(),
                 f"{v}", ha="center", va="bottom", fontsize=10)

    plt.xlabel("Cluster ID")
    plt.ylabel("Number of applications")
    plt.title("Applications per Cluster")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    out_path = out_dir / "problem2_bar_chart.png"
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"[OK] Wrote {out_path}")
    return out_path

def save_density_plot(timeline_df: DataFrame, cluster_df: DataFrame, out_dir: Path) -> Path:
    """
    problem2_density_plot.png
    - 최대 num_applications 클러스터 선택
    - 각 앱의 duration_sec = end_ts - start_ts (초)
    - 히스토그램(density=True) + KDE overlay
    - x축 log 스케일
    - 제목에 sample count (n=...) 표기
    """
    # 1) 최대 클러스터 선택
    top_row = cluster_df.orderBy(F.col("num_applications").desc(), "cluster_id").limit(1).collect()
    if not top_row:
        return out_dir / "problem2_density_plot.png"
    top_cid = top_row[0]["cluster_id"]

    # 2) 타임라인을 timestamp로 되돌리고 duration 계산
    with_ts = (
        timeline_df
        .withColumn("start_ts", F.to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("end_ts",   F.to_timestamp("end_time",   "yyyy-MM-dd HH:mm:ss"))
        .withColumn("duration_sec", F.col("end_ts").cast("long") - F.col("start_ts").cast("long"))
        .where( (F.col("cluster_id") == top_cid) & (F.col("duration_sec") > 0) )
        .select("duration_sec")
    )

    pdf = with_ts.toPandas()
    if pdf.empty:
        # 빈 경우라도 파일은 만들어 둠
        out_path = out_dir / "problem2_density_plot.png"
        plt.figure(figsize=(7, 4))
        plt.title(f"Job Duration Distribution (Cluster {top_cid}) — n=0")
        plt.xlabel("Duration (seconds, log scale)")
        plt.ylabel("Density")
        plt.savefig(out_path, dpi=150)
        plt.close()
        print(f"[OK] Wrote {out_path}")
        return out_path

    durations = pdf["duration_sec"].to_numpy()
    n = len(durations)

    plt.figure(figsize=(7, 4))
    plt.hist(durations, bins=50, density=True, alpha=0.5)

 
    try:
        from scipy.stats import gaussian_kde
        xs = np.linspace(durations.min(), durations.max(), 400)
        kde = gaussian_kde(durations.astype(float))
        ys = kde(xs)
        plt.plot(xs, ys, linewidth=2)
    except Exception:
        pass  

    plt.xscale("log")
    plt.xlabel("Duration (seconds, log scale)")
    plt.ylabel("Density")
    plt.title(f"Job Duration Distribution (Cluster {top_cid}) — n={n}")
    plt.tight_layout()

    out_path = out_dir / "problem2_density_plot.png"
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"[OK] Wrote {out_path}")
    return out_path


def write_timeline(timeline_df: DataFrame, out_dir: Path) -> Path:
    out_path = out_dir / "problem2_timeline.csv"
    timeline_df.toPandas().to_csv(out_path, index=False)
    print(f"[OK] Wrote {out_path}")
    return out_path


def write_cluster_summary(cluster_df: DataFrame, out_dir: Path) -> Path:
    out_path = out_dir / "problem2_cluster_summary.csv"
    cluster_df.toPandas().to_csv(out_path, index=False)
    print(f"[OK] Wrote {out_path}")
    return out_path


def write_stats(timeline_df: DataFrame, cluster_df: DataFrame, out_dir: Path) -> Path:

    num_clusters = cluster_df.count()
    total_apps = timeline_df.count()
    avg_apps = (total_apps / num_clusters) if num_clusters else 0.0

    topN = (
        cluster_df.select("cluster_id", "num_applications")
                  .orderBy(F.col("num_applications").desc(), F.col("cluster_id"))
                  .limit(10)
                  .collect()
    )

    lines = []
    lines.append(f"Total unique clusters: {num_clusters}")
    lines.append(f"Total applications: {total_apps}")
    lines.append(f"Average applications per cluster: {avg_apps:.2f}")
    lines.append("")
    lines.append("Most heavily used clusters:")
    for r in topN:
        lines.append(f"  Cluster {r['cluster_id']}: {r['num_applications']} applications")

    out_path = out_dir / "problem2_stats.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"[OK] Wrote {out_path}")
    return out_path


# -------------------- Main --------------------
def main():
    logger.info("Starting Problem 2 (Local Mode)")
    print("=" * 70)
    print("PROBLEM 2: Application Timeline & Cluster Summary — LOCAL MODE")
    print("=" * 70)

    master_url = None
    if len(sys.argv) > 1 and not sys.argv[1].startswith("--"):
        master_url = sys.argv[1]

    print(f"Using Spark master: {master_url or 'local[*]'}")
    logger.info(f"Using Spark master URL: {master_url or 'local[*]'}")

    t0 = time.time()
    spark = create_spark_session(master_url)

    # input path: (a) explicit second arg, else local sample
    if len(sys.argv) > 2 and not sys.argv[2].startswith("--"):
        path = sys.argv[2]
        logger.info(f"Using explicit input path from argv[2]: {path}")
    else:
        path = input_path

    try:
        raw = read_raw(spark, path)
        parsed = parse_logs_with_ids(raw)

        # Build outputs
        timeline_df = build_application_timeline(parsed)
        cluster_df  = build_cluster_summary(timeline_df)

        out_dir = _resolve_out_dir(path)
        write_timeline(timeline_df, out_dir)
        write_cluster_summary(cluster_df, out_dir)
        write_stats(timeline_df, cluster_df, out_dir)

        save_bar_chart(cluster_df, out_dir)
        save_density_plot(timeline_df, cluster_df, out_dir)

        success = True
    except Exception as e:
        logger.exception(f"Error in Problem 2: {e}")
        print(f"❌ Error in Problem 2: {e}")
        success = False
    finally:
        spark.stop()

    elapsed = time.time() - t0
    print(f"Total execution time: {elapsed:.2f} seconds")

    print("\n" + "=" * 70)
    print("✅ PROBLEM 2 COMPLETED SUCCESSFULLY!" if success else "❌ Problem 2 failed")
    if success:
        out_dir = _resolve_out_dir(path)
        print("Outputs written:")
        print(f"  {out_dir / 'problem2_timeline.csv'}")
        print(f"  {out_dir / 'problem2_cluster_summary.csv'}")
        print(f"  {out_dir / 'problem2_stats.txt'}")
        print(f"  {out_dir / 'problem2_bar_chart.png'}")
        print(f"  {out_dir / 'problem2_density_plot.png'}")
    print("=" * 70)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
