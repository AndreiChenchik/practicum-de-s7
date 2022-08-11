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

events_count = events.filter(F.col("event.message_to").isNotNull()).count()

print(events_count)

spark.stop()

# Двигайтесь дальше! Ваш код: XUdp1wiOxP
