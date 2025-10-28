#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import argparse
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, rand, to_timestamp, trim, input_file_name
)

# -------------------- Defaults --------------------
# 로컬 샘플 (마스터 노드에 다운받은 sample)
DEFAULT_LOCAL_INPUT = "file:///home/ubuntu/spark-cluster/data/sample/"
# S3 전체 데이터 (수동 전환용 예시)
# DEFAULT_LOCAL_INPUT = "s3a://<your-bucket>/data/"

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)
logger = logging.getLogger(__name__)


# -------------------- Spark --------------------
def create_spark_session(master_url: str | None):
    """
    Create a SparkSession for Problem 2.
    If master_url is provided, bind to that cluster; otherwise, use Spark's default.
    """
    builder = (
        SparkSession.builder
        .appName("Problem2-Cluster-Usage-Analysis")
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
    logger.info("Spark session created successfully")
    return spark


# -------------------- Local helpers --------------------
def local_input_glob() -> str:
    """Return file:// glob pointing to data/sample/application_*/**"""
    abs_sample = os.path.abspath("data/sample").rstrip("/")
    return f"file:///{abs_sample}/application_*/**"


def local_out_dir() -> Path:
    """Outputs go to repo-local data/output/"""
    d = Path("data/output")
    d.mkdir(parents=True, exist_ok=True)
    return d


def _to_glob(path: str) -> str:
    p = path.rstrip("/")
    return p if ("application_*" in p or p.endswith("/**")) else f"{p}/application_*/**"


def _resolve_out_dir(input_path: str) -> Path:
    if input_path.startswith("file://"):
        out = Path("data/output")
    elif input_path.startswith("s3a://"):
        out = Path.home() / "spark-cluster" / "data" / "output"
    else:
        out = Path("data/output")
    out.mkdir(parents=True, exist_ok=True)
    return out


# -------------------- Read --------------------
def read_raw_local(spark: SparkSession) -> DataFrame:
    """LOCAL TEST — read all files under data/sample/application_*/** and attach file_path."""
    read_path = local_input_glob()
    print("=" * 70)
    print("LOCAL TEST — Problem 2 (cluster usage)")
    print(f"Reading from: {read_path}")
    print("=" * 70)
    return spark.read.text(read_path).withColumn("file_path", input_file_name())


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    """Read from provided path (supports file:// or s3a://)."""
    read_path = _to_glob(path)
    print("=" * 70)
    print("CLUSTER RUN — Problem 2 (cluster usage)")
    print(f"Reading from: {read_path}")
    print("=" * 70)
    return spark.read.text(read_path).withColumn("file_path", input_file_name())


# -------------------- Parse --------------------
def parse_logs_with_ids(logs_df: DataFrame) -> DataFrame:
    """
    Parse Spark logs into columns we need for Problem 2.
    - timestamp (string) -> ts (timestamp)
    - log_level/component/message (for context)
    - file_path
    - application_id (from file_path or line content)
    - cluster_id (digits before suffix in application_id)
    - app_number (zero-padded 4-digit string)
    """
    from pyspark.sql.functions import coalesce, regexp_extract, to_timestamp, lpad

    base = logs_df.select(
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1).alias('log_level'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)\s+([^:]+):', 2).alias('component'),
        col('value').alias('message'),
        col('file_path')
    )

    app_from_path = regexp_extract('file_path', r'(application_\d+_\d+)', 1)
    app_from_msg  = regexp_extract('message',   r'(application_\d+_\d+)', 1)
    df = base.withColumn('application_id', coalesce(app_from_path, app_from_msg))

    cluster_id = regexp_extract('application_id', r'application_(\d+)_\d+', 1)
    app_num    = regexp_extract('application_id', r'application_\d+_(\d+)', 1)
    df = (df
          .withColumn('cluster_id', cluster_id)
          .withColumn('app_number', lpad(app_num, 4, '0'))
          .withColumn('ts', to_timestamp('timestamp', 'yy/MM/dd HH:mm:ss')))

    return df


# -------------------- Outputs (CSV/PNG/TXT) --------------------
def make_cluster_summary(parsed_df: DataFrame, out_dir: Path) -> Path:
    """
    Create:
      - data/output/problem2_cluster_summary.csv
      - data/output/problem2_stats.txt
      - data/output/problem2_bar_chart.png
    Return: cluster_summary CSV path
    """
    from pyspark.sql.functions import min as spark_min, max as spark_max, countDistinct
    import pandas as pd
    import matplotlib.pyplot as plt
    import seaborn as sns

    # per-application start/end
    app_times = (parsed_df
                 .where(col('application_id') != "")
                 .where(col('ts').isNotNull())
                 .groupBy('application_id', 'cluster_id')
                 .agg(spark_min('ts').alias('start_time'),
                      spark_max('ts').alias('end_time')))

    # per-cluster summary
    cluster_summary = (app_times
                       .groupBy('cluster_id')
                       .agg(
                           countDistinct('application_id').alias('num_applications'),
                           spark_min('start_time').alias('cluster_first_app'),
                           spark_max('end_time').alias('cluster_last_app')
                       ))

    cluster_summary_pd = (cluster_summary
                          .orderBy(col('num_applications').desc(), col('cluster_id'))
                          .toPandas())
    cluster_csv = out_dir / "problem2_cluster_summary.csv"
    cluster_summary_pd.to_csv(cluster_csv, index=False)
    print(f"[OK] Wrote {cluster_csv}")

    # stats txt
    total_clusters = int(cluster_summary_pd['cluster_id'].nunique()) if not cluster_summary_pd.empty else 0
    total_apps = int(cluster_summary_pd['num_applications'].sum()) if not cluster_summary_pd.empty else 0
    avg_apps = total_apps / total_clusters if total_clusters else 0.0

    top_lines = []
    if not cluster_summary_pd.empty:
        top_sorted = cluster_summary_pd.sort_values('num_applications', ascending=False)
        for _, row in top_sorted.iterrows():
            top_lines.append(f"  Cluster {row['cluster_id']}: {int(row['num_applications'])} applications")

    stats_txt = out_dir / "problem2_stats.txt"
    with open(stats_txt, "w") as f:
        f.write(f"Total unique clusters: {total_clusters}\n")
        f.write(f"Total applications: {total_apps}\n")
        f.write(f"Average applications per cluster: {avg_apps:.2f}\n\n")
        f.write("Most heavily used clusters:\n")
        for line in top_lines[:10]:
            f.write(line + "\n")
    print(f"[OK] Wrote {stats_txt}")

    # bar chart
    bar_png = out_dir / "problem2_bar_chart.png"
    if not cluster_summary_pd.empty:
        plt.figure(figsize=(10, 6))
        ax = sns.barplot(data=cluster_summary_pd.sort_values('num_applications', ascending=False),
                         x='cluster_id', y='num_applications')
        for p in ax.patches:
            height = p.get_height()
            ax.annotate(f"{int(height)}",
                        (p.get_x() + p.get_width()/2., height),
                        ha='center', va='bottom', xytext=(0, 3), textcoords='offset points')
        ax.set_xlabel("Cluster ID")
        ax.set_ylabel("Number of Applications")
        ax.set_title("Applications per Cluster")
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(bar_png, dpi=150)
        plt.close()
        print(f"[OK] Wrote {bar_png}")
    else:
        print("[WARN] No data for bar chart; skipping PNG.")

    return cluster_csv


def make_timeline_and_density(parsed_df: DataFrame, out_dir: Path) -> Path:
    """
    Create:
      - data/output/problem2_timeline.csv
      - data/output/problem2_density_plot.png
    Return: timeline CSV path
    """
    from pyspark.sql.functions import min as spark_min, max as spark_max, regexp_extract, lpad
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import seaborn as sns

    # per-application start/end
    app_times = (parsed_df
                 .where(col('application_id') != "")
                 .where(col('ts').isNotNull())
                 .groupBy('cluster_id', 'application_id')
                 .agg(
                     spark_min('ts').alias('start_time'),
                     spark_max('ts').alias('end_time')
                 ))

    # app_number for display
    app_times = (app_times
                 .withColumn('app_number',
                             lpad(regexp_extract('application_id', r'application_\d+_(\d+)', 1), 4, '0')))

    timeline_pd = (app_times
                   .select('cluster_id', 'application_id', 'app_number', 'start_time', 'end_time')
                   .orderBy(col('cluster_id'), col('start_time'))
                   .toPandas())

    timeline_csv = out_dir / "problem2_timeline.csv"
    timeline_pd.to_csv(timeline_csv, index=False)
    print(f"[OK] Wrote {timeline_csv}")

    # density plot for the largest cluster
    if not timeline_pd.empty:
        timeline_pd['duration_sec'] = (pd.to_datetime(timeline_pd['end_time']) -
                                       pd.to_datetime(timeline_pd['start_time'])).dt.total_seconds()

        if timeline_pd['cluster_id'].nunique() > 0:
            biggest_cluster = (timeline_pd
                               .groupby('cluster_id')['application_id']
                               .nunique().sort_values(ascending=False).index[0])

            biggest_df = timeline_pd[timeline_pd['cluster_id'] == biggest_cluster].copy()
            biggest_df = biggest_df[np.maximum(biggest_df['duration_sec'], 0) >= 0]

            density_png = out_dir / "problem2_density_plot.png"
            if not biggest_df.empty:
                plt.figure(figsize=(10, 6))
                ax = sns.histplot(biggest_df['duration_sec'], kde=True, bins=30)
                ax.set_xscale('log')
                ax.set_xlabel("Application Duration (seconds, log scale)")
                ax.set_ylabel("Count")
                ax.set_title(f"Duration Distribution — Cluster {biggest_cluster} (n={len(biggest_df)})")
                plt.tight_layout()
                plt.savefig(density_png, dpi=150)
                plt.close()
                print(f"[OK] Wrote {density_png}")
            else:
                print("[WARN] No durations for largest cluster; skipping density plot.")
        else:
            print("[WARN] No clusters found; skipping density plot.")
    else:
        print("[WARN] Timeline is empty; skipping density plot.")

    return timeline_csv


# -------------------- CLI & Main --------------------
def parse_args():
    p = argparse.ArgumentParser(
        description="Problem 2: Cluster Usage Analysis — build timeline & summaries & plots"
    )
    p.add_argument("master_or_path", nargs="?", default=None,
                   help="Spark master URL (e.g., spark://x.x.x.x:7077) or explicit input path (file:// or s3a://).")
    p.add_argument("maybe_input_path", nargs="?", default=None,
                   help="Optional explicit input path if first arg was a master URL.")
    p.add_argument("--net-id", dest="net_id", default=None,
                   help="If provided, use s3a://<NETID>-assignment-spark-cluster-logs/data/")
    p.add_argument("--skip-spark", action="store_true",
                   help="Skip Spark; regenerate visualizations/stats from existing CSVs.")
    return p.parse_args()


def main():
    logger.info("Starting Problem 2")
    print("=" * 70)
    print("PROBLEM 2: Cluster Usage Analysis")
    print("=" * 70)

    # 1) master_url: 첫 번째 위치 인수(옵션) 처리
    #    - "local[*]" / "local" 지원
    #    - "spark://..." 지원
    #    - IP 형식(x.x.x.x)이면 spark://IP:7077로 처리
    if len(sys.argv) > 1 and not sys.argv[1].startswith("--"):
        a1 = sys.argv[1]
        if a1 in ("local[*]", "local"):
            master_url = a1
        elif a1.startswith("spark://"):
            master_url = a1
        elif a1.count(".") == 3:  # e.g., 10.0.0.12
            master_url = f"spark://{a1}:7077"
        elif a1.startswith("file://") or a1.startswith("s3a://"):
            # 첫 인수가 경로로 들어온 케이스: 마스터는 환경변수/후속 절차로 처리
            master_url = None
        else:
            master_url = None
    else:
        master_url = None

    # 환경변수 MASTER_PRIVATE_IP로 보완
    if not master_url:
        master_private_ip = os.getenv("MASTER_PRIVATE_IP")
        if master_private_ip:
            master_url = f"spark://{master_private_ip}:7077"

    if not master_url:
        print("❌ Error: Master URL not provided")
        print("Run: python ~/problem2.py spark://$MASTER_PRIVATE_IP:7077 [INPUT_PATH] [--net-id YOUR_ID]")
        print("  or: python ~/problem2.py local[*] $ABS/data/sample")
        print("  or: export MASTER_PRIVATE_IP=xxx.xxx.xxx.xxx")
        return 1

    print(f"Connecting to Spark Master at: {master_url}")
    logger.info(f"Using Spark master URL: {master_url}")

    t0 = time.time()
    logger.info("Initializing Spark session for cluster execution")
    spark = create_spark_session(master_url)

    # 2) 입력 경로 우선순위
    #    (a) 두 번째 위치 인수(가장 우선). file:// 또는 s3a:// 없으면 file://로 보정
    #    (b) --net-id -> s3a://<id>-assignment-spark-cluster-logs/data/
    #    (c) SPARK_LOGS_BUCKET (단, (a)로 명시했거나 local 모드면 덮어쓰지 않음)
    #    (d) 파일 상단의 기본 input_path
    explicit_path = False
    if len(sys.argv) > 2 and not sys.argv[2].startswith("--"):
        raw_path = sys.argv[2]
        if not (raw_path.startswith("file://") or raw_path.startswith("s3a://")):
            raw_path = "file://" + os.path.abspath(raw_path)
        path = raw_path
        explicit_path = True
        logger.info(f"Using explicit input path from argv[2]: {path}")
    else:
        path = input_path  # 파일 상단 기본값

    # --net-id 처리(경로를 명시하지 않은 경우에만)
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

        # SPARK_LOGS_BUCKET은 명시 경로 없고, 로컬 모드가 아닐 때만 적용
        env_bucket = os.getenv("SPARK_LOGS_BUCKET")
        if env_bucket and not net_id and not explicit_path and master_url not in ("local[*]", "local"):
            bucket_a = env_bucket.replace("s3://", "s3a://", 1)
            path = f"{bucket_a}/data/"
            logger.info(f"SPARK_LOGS_BUCKET detected; overriding path to: {path}")

    try:
        logger.info("Problem 2 — building outputs")
        # 당신 코드에 read_raw(spark, path)가 이미 있다면 사용
        raw = read_raw(spark, path)  # 없다면 read_any/read_raw_local로 대체
        parsed = parse_logs_with_ids(raw)

        out_dir = _resolve_out_dir(path)

        # Problem 2 산출물 생성
        make_timeline_and_density(parsed, out_dir)  # problem2_timeline.csv + density PNG
        make_cluster_summary(parsed, out_dir)       # cluster_summary.csv + stats.txt + bar PNG

        success = True
        logger.info("Problem 2 outputs created successfully")
    except Exception as e:
        logger.exception(f"Error occurred while solving Problem 2: {e}")
        print(f"❌ Error solving Problem 2: {e}")
        success = False
    finally:
        try:
            spark.stop()
        except Exception:
            pass

    elapsed = time.time() - t0
    logger.info(f"Total execution time: {elapsed:.2f} seconds")
    print(f"Total execution time: {elapsed:.2f} seconds")

    print("\n" + "=" * 70)
    print("✅ PROBLEM 2 COMPLETED SUCCESSFULLY!" if success else "❌ Problem 2 failed")
    if success:
        print(f"  {out_dir / 'problem2_timeline.csv'}")
        print(f"  {out_dir / 'problem2_cluster_summary.csv'}")
        print(f"  {out_dir / 'problem2_stats.txt'}")
        print(f"  {out_dir / 'problem2_bar_chart.png'}")
        print(f"  {out_dir / 'problem2_density_plot.png'}")
    print("=" * 70)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())