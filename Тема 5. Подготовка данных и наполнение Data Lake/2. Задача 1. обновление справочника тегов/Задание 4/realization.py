from datetime import datetime, timedelta
from typing import List
import sys

import pyspark
from pyspark.sql import SparkSession

import pyspark.sql.functions as F

DATE_FORMAT = "%Y-%m-%d"


def main():
    date_str = sys.argv[1]
    depth = int(sys.argv[2])
    threshold = int(sys.argv[3])
    events_path = sys.argv[4]
    tags_path = sys.argv[5]
    output_path = sys.argv[6]

    app_name = f"VerifiedTagsCandidatesJob-{date_str}-d{depth}-cut{threshold}"
    print(app_name)

    spark = SparkSession.builder.master("local").appName(app_name).getOrCreate()

    date = datetime.strptime(date_str, "%Y-%m-%d")
    save_updated_tags(
        session=spark,
        events_path=events_path,
        tags_path=tags_path,
        output_path=output_path,
        date=date,
        depth=depth,
        threshold=threshold,
    )


def date_to_str(date: datetime) -> str:
    return date.strftime(DATE_FORMAT)


def input_dates(date: datetime, depth: int) -> List[str]:
    return [date_to_str(date - timedelta(days=d)) for d in range(depth)]


def save_updated_tags(
    session: SparkSession,
    events_path: str,
    tags_path: str,
    output_path: str,
    date: datetime,
    depth: int,
    threshold: int,
):
    verified_tags = session.read.parquet(f"{tags_path}")
    events = session.read.parquet(f"{events_path}")
    dates = input_dates(date, depth)

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
        .filter(f"suggested_count >= {threshold}")
    )

    filtered_tags = user_tags.join(
        verified_tags, user_tags.tag == verified_tags.tag, how="left_anti"
    )

    filtered_tags.write.option("header", True).mode("overwrite").parquet(
        f"{output_path}/date={date_to_str(date)}"
    )


if __name__ == "__main__":
    main()

# Двигайтесь дальше! Ваш код: oKGR1Y9EeT
