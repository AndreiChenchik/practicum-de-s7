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

events = events.withColumn("hour", F.hour(F.col("event.datetime")))
events = events.withColumn("minute", F.minute(F.col("event.datetime")))
events = events.withColumn("second", F.second(F.col("event.datetime")))

events = events.orderBy(F.col("event.datetime").desc())

print(events.show(10))

spark.stop()

# Двигайтесь дальше! Ваш код: 5T3T8hAzm0
