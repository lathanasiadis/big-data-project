import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, dense_rank, col, to_timestamp
from pyspark.sql.functions import year, month, when, hour
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from utils import *

app = SparkSession.builder.appName("question4dataframe").getOrCreate()
app.sparkContext.setLogLevel("WARN")

t0 = time.perf_counter()
df1 = app.read.csv(CSV_1, header=True, inferSchema=True)
df2 = app.read.csv(CSV_2, header=True, inferSchema=True)
df = df1.union(df2)
t1 = time.perf_counter()

df = df.filter(col("Premis Desc") == "STREET")
df = df.withColumn("time cat", (
            when((col("TIME OCC") >= 500) & (col("TIME OCC") < 1200), "Morning")
           .when((col("TIME OCC") >= 1200) & (col("TIME OCC") < 1700), "Afternoon")
           .when((col("TIME OCC") >= 1700) & (col("TIME OCC") < 2100), "Evening")
           .otherwise("Night")))

df = df.groupby(col("time cat")).agg(F.count("*").alias("num crimes")) \
       .orderBy(col("num crimes").desc())

results = df.collect()
t2 = time.perf_counter()

print("[Query 2 - Dataframes] Data loading done in {:.2f}s".format(t1-t0))
print("[Query 2 - Dataframes] Query done in {:.2f}s".format(t2-t1))
print("Results:\n{}".format(results))

