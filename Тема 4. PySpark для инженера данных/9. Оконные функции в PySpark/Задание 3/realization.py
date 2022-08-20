import findspark

findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.master("yarn")
    .appName("Learning DataFrames")
    .getOrCreate()
)

events = spark.read.json("/user/master/data/events")

window = Window().partitionBy("event.message_from").orderBy("date")

dfWithLag = events.withColumn(
    "lag_7", F.lag("event.message_to", 7).over(window)
)

result = (
    dfWithLag.select("event.message_from", "date", "lag_7")
    .filter(dfWithLag.lag_7.isNotNull())
    .orderBy(F.desc("date"))
)

result.show(10, False)

spark.stop()

# Двигайтесь дальше! Ваш код: tl0Bn1k6Aw
