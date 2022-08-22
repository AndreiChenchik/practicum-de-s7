import sys

import findspark

findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession

import pyspark.sql.functions as F


def main():
    base_input_path = sys.argv[1]
    date = sys.argv[2]
    base_output_path = sys.argv[3]

    spark = (
        SparkSession.builder.master("yarn")
        .appName("Learning DataFrames")
        .getOrCreate()
    )

    events = spark.read.json(f"{base_input_path}/date={date}")

    parquet_events = (
        events.repartition("date", "event_type")
        .write.option("header", True)
        .partitionBy("date", "event_type")
        .mode("overwrite")
        .parquet("/user/andrei/data/events")
    )

    latest_events = parquet_events.orderBy(F.desc("event.datetime")).show()
    print(latest_events)

    spark.stop()


if __name__ == "__main__":
    main()


# для теста:
# partitionBy('event.event_type')
# sql.read.json
# format('parquet')
# save

# Двигайтесь дальше! Ваш код: DJhlUTi6G5
