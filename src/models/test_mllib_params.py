from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier

spark = SparkSession.builder.master("local[2]").appName("TestMLlibParams").getOrCreate()
rf = RandomForestClassifier()
gbt = GBTClassifier()
print("RF has weightCol:", rf.hasParam("weightCol"))
print("GBT has weightCol:", gbt.hasParam("weightCol"))
spark.stop()
