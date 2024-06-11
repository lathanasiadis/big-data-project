import time
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, dense_rank, col, to_timestamp
from pyspark.sql.functions import year, month
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from utils import CSV_1, CSV_2, PARQUET_1, PARQUET_2

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--format",
    help="'csv' or 'parquet'",
    default='csv')
args = parser.parse_args()

if args.format != 'csv' and args.format != 'parquet':
    raise Exception("Invalid format argument, must be 'csv' or 'parquet'")

app = SparkSession.builder.appName("question3dataframe").getOrCreate()
app.sparkContext.setLogLevel("WARN")

print("Measuring times for the DataFrame API")
t0 = time.perf_counter()

if args.format == 'csv':
    df1 = app.read.csv(CSV_1, header=True, inferSchema=True)
    df2 = app.read.csv(CSV_2, header=True, inferSchema=True)
else:
    df1 = app.read.parquet(PARQUET_1)
    df2 = app.read.parquet(PARQUET_2)

t1 = time.perf_counter()

window = (Window.partitionBy("Year")
             .orderBy(col("Year").asc(), col("Crime Total").desc()))

df = (df1.union(df2)
      .withColumn("Date", to_timestamp(col("DATE OCC"), 'MM/dd/yyyy hh:mm:ss a'))
      .withColumn("Year", year(col("Date")))
      .withColumn("Month", month(col("Date")))
      .groupby("Year", "Month").agg(F.count("*").alias("Crime Total"))
      .withColumn("Ranking", dense_rank().over(window))
      .filter(col("Ranking") <= 3)
     )


df.show()
t2 = time.perf_counter()

if args.format == 'csv':
    print(f"Loading time for the CSV format: {round(t1 - t0, 2)}s")
    print(f"Query time for the CSV format: {round(t2 - t1, 2)}s")
else:
    print(f"Loading time for the Parquet format: {round(t1 - t0, 2)}s")
    print(f"Query time for the Parquet format: {round(t2 - t1, 2)}s")

app.stop()

