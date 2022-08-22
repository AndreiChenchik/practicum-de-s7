from datetime import datetime, timedelta
from typing import List

import findspark

findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


DATE_FORMAT = "%Y-%m-%d"  # yyyy-MM-dd


def date_to_str(date: datetime) -> str:
    return date.strftime(DATE_FORMAT)


def input_dates(date_str: str, depth: int) -> List[str]:
    date = datetime.strptime(date_str, DATE_FORMAT)
    return [date_to_str(date - timedelta(days=d)) for d in range(depth)]


spark = (
    SparkSession.builder.master("yarn")
    .appName("Learning DataFrames")
    .getOrCreate()
)

verified_tags = spark.read.parquet(
    "/user/master/data/snapshots/tags_verified/actual"
)
events = spark.read.parquet("/user/andrei/data/events")
dates = input_dates("2022-05-31", 7)

filtered_events = events.filter(
    (events.date.isin(dates))
    & (events.event_type == "message")
    & (events.event.message_channel_to.isNotNull())
)

user_tags = (
    filtered_events.select(
        F.col("event.message_from").alias("author"),
        F.explode("event.tags").alias("tag"),
    )
    .groupBy("tag")
    .agg(F.countDistinct("author").alias("suggested_count"))
    .filter("suggested_count >= 100")
)

filtered_tags = user_tags.join(
    verified_tags, user_tags.tag == verified_tags.tag, how="left_anti"
)

filtered_tags.write.option("header", True).mode("overwrite").parquet(
    "/user/andrei/data/analytics/candidates_d7_pyspark"
)

spark.stop()

# для теста:
# where

# Двигайтесь дальше! Ваш код: yuYyLHDDSz
