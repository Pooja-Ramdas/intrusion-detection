import time

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.types import DoubleType, FloatType, NumericType

from common import HDFS, RAW, day_column, find_column, label_and_day, normalize_columns, path_exists, spark_session, write_parquet_once, write_small_table


OUT = f"{HDFS}/bda/clean/phase2_v1"
SAMPLE_LIMIT = 50_000


def clean_one(spark, dataset, path):
    started = time.time()
    raw = normalize_columns(spark.read.parquet(path).limit(SAMPLE_LIMIT))
    tagged = label_and_day(raw, dataset)
    if dataset == "cicids2017_packet" and find_column(tagged, ["timestamp", "packet_timestamp", "time"]) is None:
        packet_key = find_column(tagged, ["flow_id"])
        flow = normalize_columns(spark.read.parquet(f"{RAW}/cicids2017_flow/CICIDS_Flow.parquet").limit(SAMPLE_LIMIT))
        flow_key = find_column(flow, ["flow_id"])
        if packet_key and flow_key:
            flow_days = flow.select(F.col(flow_key).cast("string").alias("_flow_key"), day_column(flow, "cicids2017_flow").alias("_mapped_day")).dropDuplicates(["_flow_key"])
            tagged = tagged.drop("day").join(broadcast(flow_days), F.col(packet_key).cast("string") == F.col("_flow_key"), "left")
            tagged = tagged.withColumn("day", F.coalesce(F.col("_mapped_day"), F.lit("UNKNOWN"))).drop("_flow_key", "_mapped_day")
    rows_before = tagged.count()
    profile_summary = f"hdfs://namenode:9000/bda/_meta/profile/phase2_v1/profile_summary"
    measured_dups = None
    if path_exists(spark, profile_summary):
        row = spark.read.parquet(profile_summary).filter(F.col("dataset") == dataset).select("exact_duplicate_rows").first()
        measured_dups = int(row[0]) if row else None
    if measured_dups == 0:
        deduped = tagged
        rows_after_dedupe = rows_before
    else:
        deduped = tagged.dropDuplicates(tagged.columns)
        rows_after_dedupe = deduped.count()
    audit = [(dataset, "exact_duplicate_rows", rows_before - rows_after_dedupe)]
    identity = find_column(deduped, ["packet_id", "flow_id", "id", "record_id"])
    if identity:
        row_key = F.sha2(F.concat_ws("|", F.lit(dataset), F.input_file_name(), F.col(identity).cast("string")), 256)
    else:
        row_key = F.sha2(F.to_json(F.struct(*[F.col(f"`{c.replace('`', '``')}`") for c in deduped.columns])), 256)
    deduped = deduped.withColumn("split_key", row_key)
    duration = find_column(deduped, ["flow_duration", "duration"])
    if duration:
        negative = deduped.filter(F.col(duration).cast("double") < 0).count()
        audit.append((dataset, "negative_duration_rows_repaired_to_null", negative))
        deduped = deduped.withColumn(duration, F.when(F.col(duration).cast("double") < 0, None).otherwise(F.col(duration)))
    # Repair non-finite numeric values to null, preserving rows and an auditable repair count.
    numeric = [f for f in deduped.schema.fields if isinstance(f.dataType, NumericType)]
    repaired_count_exprs = []
    impossible_exprs = []
    replacements = {}
    for field in numeric:
        col = F.col(f"`{field.name.replace('`', '``')}`")
        name = field.name.lower()
        x = col.cast("double")
        range_invalid = F.lit(False)
        if "port" in name:
            range_invalid = (x < 0) | (x > 65535)
        elif name.startswith("packet_byte_"):
            range_invalid = (x < 0) | (x > 255)
        elif any(token in name for token in ("duration", "length", "bytes", "packets", "packet_count", "flow_count")):
            range_invalid = x < 0
        invalid = range_invalid
        if isinstance(field.dataType, (FloatType, DoubleType)):
            repaired_count_exprs.append(F.sum(F.when(F.isnan(x) | (F.abs(x) == float("inf")), 1).otherwise(0)).alias(f"{field.name}__nonfinite"))
            invalid = invalid | F.isnan(x) | (F.abs(x) == float("inf"))
        impossible_exprs.append(F.sum(F.when(range_invalid, 1).otherwise(0)).alias(f"{field.name}__impossible"))
        replacements[field.name] = F.when(invalid, None).otherwise(col).alias(field.name)
    if replacements:
        deduped = deduped.select(*[replacements.get(c, F.col(f"`{c.replace('`', '``')}`")) for c in deduped.columns])
    # One aggregate pass measures both repair categories before changing values.
    metrics = tagged.agg(*(repaired_count_exprs + impossible_exprs)).first().asDict() if (repaired_count_exprs or impossible_exprs) else {}
    if repaired_count_exprs:
        audit.append((dataset, "nonfinite_numeric_cells_repaired_to_null",
                      sum(int(metrics.get(f"{f.name}__nonfinite") or 0) for f in numeric if isinstance(f.dataType, (FloatType, DoubleType)))))
    if impossible_exprs:
        audit.append((dataset, "impossible_numeric_cells_repaired_to_null",
                      sum(int(metrics.get(f"{f.name}__impossible") or 0) for f in numeric)))
    # Prevent identifier and direct-identity leakage into model tables.
    leakage = [c for c in deduped.columns if c in {
        "source_ip", "destination_ip", "src_ip", "dst_ip", "source_port", "destination_port",
        "src_port", "dst_port", "timestamp", "flow_id", "packet_id", "flow_id_dup2", "id"
    }]
    if leakage:
        deduped = deduped.drop(*leakage)
    # Drop constants using the measured profile. If absent, retain columns and report limitation.
    quality_path = f"hdfs://namenode:9000/bda/_meta/profile/phase2_v1/column_quality"
    dropped_constants = []
    if path_exists(spark, quality_path):
        q = spark.read.parquet(quality_path).filter((F.col("dataset") == dataset) & F.col("is_constant"))
        dropped_constants = [r[0] for r in q.select("column_name").collect() if r[0] in deduped.columns and r[0] not in ("label_multi", "label_binary", "day")]
        if dropped_constants:
            deduped = deduped.drop(*dropped_constants)
    # No cleaning operation after exact de-duplication removes rows; repairs and column drops preserve them.
    after = rows_after_dedupe
    audit.extend([(dataset, "rows_before_cleaning", rows_before),
                  (dataset, "constant_columns_dropped", len(dropped_constants)),
                  (dataset, "leakage_columns_dropped", len(leakage)),
                  (dataset, "rows_after_cleaning", after)])
    dest = f"{OUT}/{dataset}"
    if path_exists(spark, dest):
        raise RuntimeError(f"Refusing to overwrite existing clean destination: {dest}")
    # 96 shuffle partitions bounds per-task input while day/class partitioning groups output.
    cleaned = deduped.repartition(96, "day", "label_multi")
    write_parquet_once(spark, cleaned, dest, ["day", "label_multi"])
    counts = deduped.withColumn("dataset", F.lit(dataset)).groupBy("dataset", "day", "label_multi", "label_binary").count().withColumnRenamed("count", "rows")
    write_small_table(spark, counts, f"clean_counts_{dataset}")
    print(f"CLEAN dataset={dataset} sample_limit={SAMPLE_LIMIT} before={rows_before} deduped={rows_after_dedupe} after={after} output={dest} seconds={time.time()-started:.2f}", flush=True)
    return audit


def main():
    spark = spark_session("BDA-Phase2-Clean")
    spark.sparkContext.setLogLevel("WARN")
    inputs = [("cicids2017_flow", f"{RAW}/cicids2017_flow/CICIDS_Flow.parquet"),
              ("cicids2017_packet", f"{RAW}/cicids2017_packet/cicids2017_packet_slice.parquet"),
              ("unswnb15", f"{RAW}/unswnb15")]
    for dataset, _ in inputs:
        if path_exists(spark, f"{OUT}/{dataset}"):
            raise RuntimeError(f"Versioned clean dataset already exists; refusing overwrite: {OUT}/{dataset}")
    audit = []
    for dataset, path in inputs:
        audit.extend(clean_one(spark, dataset, path))
    audit_df = spark.createDataFrame(audit, "dataset string, reason string, count long")
    write_small_table(spark, audit_df, "clean_audit")
    spark.stop()


if __name__ == "__main__":
    main()
