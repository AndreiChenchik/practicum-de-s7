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

events.printSchema()

spark.stop()

# Двигайтесь дальше! Ваш код: ESLk6ckjLB
