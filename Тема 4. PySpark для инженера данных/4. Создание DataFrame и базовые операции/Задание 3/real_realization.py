import findspark
import os

findspark.init()
findspark.find()
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/usr/local/lib/python3.8"

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = (
    SparkSession.builder.master("local")
    .appName("Learning DataFrames")
    .getOrCreate()
)

df = spark.read.parquet("/user/master/data/snapshots/channels/actual")

df.write.partitionBy("channel_type").mode("append").parquet(
    "/user/AndreiChenchik/analytics/test"
)


result = (
    spark.read.parquet("/user/AndreiChenchik/analytics/test")
    .select("channel_type")
    .orderBy("channel_type")
    .distinct()
    .show()
)

print(result)
