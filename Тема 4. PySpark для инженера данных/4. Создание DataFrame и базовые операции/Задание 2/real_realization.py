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

result = spark.read.json("/user/master/data/events").show()

print(result)
