import json
import os
import re
import unicodedata
import csv
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T


HDFS = "hdfs://namenode:9000"
RAW = f"{HDFS}/bda/raw"
PHASE = "phase2_v1"
META_HDFS = f"{HDFS}/bda/_meta/profile/{PHASE}"
LOG_ROOT = Path("/logs/profile") / PHASE


def spark_session(name):
    Path("/logs/spark-events").mkdir(parents=True, exist_ok=True)
    LOG_ROOT.mkdir(parents=True, exist_ok=True)
    return (
        SparkSession.builder.appName(name)
        .config("spark.hadoop.fs.defaultFS", HDFS)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.codegen.maxFields", "100")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///logs/spark-events")
        .getOrCreate()
    )


def normalize_token(value):
    value = unicodedata.normalize("NFKC", str(value)).strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value).strip("_")
    return re.sub(r"_+", "_", value) or "unnamed"


def normalize_columns(df):
    names = []
    used = {}
    for field in df.schema.fields:
        base = normalize_token(field.name)
        used[base] = used.get(base, 0) + 1
        names.append(base if used[base] == 1 else f"{base}_dup{used[base]}")
    return df.toDF(*names)


def find_column(df, candidates, required=False):
    by_name = {c.lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in by_name:
            return by_name[candidate.lower()]
    if required:
        raise ValueError(f"Missing expected column; looked for {candidates}; have {df.columns[:30]}")
    return None


def canonical_label(raw, dataset):
    text = F.trim(F.coalesce(raw.cast("string"), F.lit("")))
    lower = F.lower(text)
    compact = F.regexp_replace(lower, r"[^a-z0-9]+", "")
    benign = compact.isin("benign", "normal", "0")
    if dataset == "unswnb15":
        return (
            F.when(benign, F.lit("BENIGN"))
            .when(text == "1", F.lit("UNSW_ATTACK"))
            .when(text == "", F.lit("UNMAPPED"))
            .otherwise(F.concat(F.lit("UNSW_"), F.regexp_replace(F.upper(text), r"[^A-Z0-9]+", "_")))
        )
    return (
        F.when(benign, F.lit("BENIGN"))
        .when(compact.contains("bruteforce"), F.lit("Web_Attack_Brute_Force"))
        .when(compact.contains("sqlinjection") | compact.contains("sqli"), F.lit("Web_Attack_Sql_Injection"))
        .when(compact.contains("xss") | compact.contains("crosssitescripting"), F.lit("Web_Attack_XSS"))
        .when(compact.contains("heartbleed"), F.lit("Heartbleed"))
        .when(compact.contains("infiltration"), F.lit("Infiltration"))
        .when(compact.contains("sshpatator"), F.lit("SSH_Patator"))
        .when(compact.contains("ftppatator"), F.lit("FTP_Patator"))
        .when(compact.contains("slowhttptest"), F.lit("DoS_Slowhttptest"))
        .when(compact.contains("slowloris"), F.lit("DoS_Slowloris"))
        .when(compact.contains("goldeneye"), F.lit("DoS_GoldenEye"))
        .when(compact.contains("hulk"), F.lit("DoS_Hulk"))
        .when(compact.contains("portscan"), F.lit("PortScan"))
        .when(compact.contains("ddos"), F.lit("DDoS"))
        .when(compact.contains("bot"), F.lit("Bot"))
        .when(text == "", F.lit("UNMAPPED"))
        .otherwise(F.concat(F.lit("UNMAPPED_"), F.regexp_replace(F.upper(text), r"[^A-Z0-9]+", "_")))
    )


def binary_label(canonical):
    return F.when(canonical == "BENIGN", F.lit("BENIGN")).otherwise(F.lit("ATTACK"))


def day_column(df, dataset):
    if dataset == "unswnb15":
        return F.lit("UNSW_external")
    timestamp_col = find_column(df, ["timestamp", "flow_start", "start_time", "date_time"])
    if timestamp_col:
        token = F.split(F.col(timestamp_col).cast("string"), r"\s+")[0]
        return (
            F.when(F.lower(token).startswith("mon"), "Monday")
            .when(F.lower(token).startswith("tue"), "Tuesday")
            .when(F.lower(token).startswith("wed"), "Wednesday")
            .when(F.lower(token).startswith("thu"), "Thursday")
            .when(F.lower(token).startswith("fri"), "Friday")
            .when(token.isin("3/7/2017", "03/07/2017"), "Monday")
            .when(token.isin("4/7/2017", "04/07/2017"), "Tuesday")
            .when(token.isin("5/7/2017", "05/07/2017"), "Wednesday")
            .when(token.isin("6/7/2017", "06/07/2017"), "Thursday")
            .when(token.isin("7/7/2017", "07/07/2017"), "Friday")
            .otherwise("UNKNOWN")
        )
    return F.lit("UNKNOWN")


def fs_path(spark, path):
    jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    return jpath.getFileSystem(spark._jsc.hadoopConfiguration()), jpath


def path_exists(spark, path):
    fs, jpath = fs_path(spark, path)
    return bool(fs.exists(jpath))


def write_parquet_once(spark, df, path, partition_cols=None):
    if path_exists(spark, path):
        fs, jpath = fs_path(spark, path)
        success = spark._jvm.org.apache.hadoop.fs.Path(jpath, "_SUCCESS")
        if fs.exists(success):
            print(f"OUTPUT_EXISTS verified _SUCCESS; preserving existing: {path}", flush=True)
            return False
        raise RuntimeError(f"Output path exists without _SUCCESS; preserving it for inspection: {path}")
    writer = df.write.mode("errorifexists").option("compression", "snappy")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    return True


def write_hdfs_json(spark, path, payload):
    if path_exists(spark, path):
        print(f"OUTPUT_EXISTS preserving {path}", flush=True)
        return False
    fs, jpath = fs_path(spark, path)
    stream = fs.create(jpath, False)
    try:
        stream.write(bytearray(json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")))
        stream.hflush()
    finally:
        stream.close()
    return True


def write_small_table(spark, df, name):
    hdfs_path = f"{META_HDFS}/{name}"
    write_parquet_once(spark, df, hdfs_path)
    csv_path = LOG_ROOT / "csv" / f"{name}.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if csv_path.exists():
        print(f"CSV_EXISTS preserving {csv_path}", flush=True)
        return str(csv_path)
    # Tables are intentionally small. Collect only summary rows at the driver so
    # executors do not need the app-only /logs bind mount.
    temp_path = csv_path.with_suffix(".csv.tmp")
    if temp_path.exists():
        raise RuntimeError(f"Refusing to overwrite partial local CSV: {temp_path}")
    rows = df.collect()
    with temp_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=df.columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.asDict(recursive=True))
    temp_path.replace(csv_path)
    return str(csv_path)


def label_and_day(df, dataset):
    df = normalize_columns(df)
    if dataset == "unswnb15":
        label = find_column(df, ["attack_cat", "attack_category", "label"], required=True)
    else:
        label = find_column(df, ["attack_label", "label", "label_raw", "class"], required=True)
    return (
        df.withColumn("label_raw", F.col(label).cast("string"))
        .withColumn("label_multi", canonical_label(F.col(label), dataset))
        .withColumn("label_binary", binary_label(F.col("label_multi")))
        .withColumn("day", day_column(df, dataset))
    )


def safe_identifier(name):
    return normalize_token(name)[:100]
