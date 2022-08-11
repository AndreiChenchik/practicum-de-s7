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
reaction_events = events.filter(F.col("event_type") == "reaction")
reactions_by_date = reaction_events.groupBy(F.col("date")).count()
max_reactions = reactions_by_date.select(F.max("count")).show()

print(max_reactions)

spark.stop()

# Двигайтесь дальше! Ваш код: lXFKRx00xQ
