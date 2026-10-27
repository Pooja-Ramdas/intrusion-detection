from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common import HDFS, spark_session, write_small_table


def main():
    spark = spark_session("BDA-Phase2-CorrectClassImbalance")
    spark.sparkContext.setLogLevel("WARN")
    source = f"{HDFS}/bda/_meta/profile/phase2_v1/rows_by_dataset_day_label"
    counts = spark.read.parquet(source)
    corrected = (counts.groupBy("dataset", "label_multi", "label_binary")
                 .agg(F.sum("rows").alias("rows"))
                 .withColumn("max_to_class_ratio", F.max("rows").over(Window.partitionBy("dataset")) / F.col("rows"))
                 .orderBy("dataset", F.desc("rows")))
    write_small_table(spark, corrected, "class_imbalance_by_class")
    print("CLASS_IMBALANCE_CORRECTED output=class_imbalance_by_class", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
