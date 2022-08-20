import findspark

findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession

import pyspark.sql.functions as F

spark = (
    SparkSession.builder.master("yarn")
    .appName("Learning DataFrames")
    .getOrCreate()
)

events = spark.read.json("/user/master/data/events")

events.write.option("header", True).partitionBy("date", "event_type").mode(
    "overwrite"
).parquet("/user/andrei/data/events")

events.orderBy(F.desc("event.datetime")).show()

spark.stop()

# Двигайтесь дальше! Ваш код: MisYAUZkzJ
