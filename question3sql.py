import time
import argparse
from pyspark.sql import SparkSession
from utils import CSV_1, CSV_2, PARQUET_1, PARQUET_2

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--format",
    help="'csv' or 'parquet'",
    default='csv')
args = parser.parse_args()

if args.format != 'csv' and args.format != 'parquet':
    raise Exception("Invalid format argument, must be 'csv' or 'parquet'")

print("Measuring times for the SQL API")
app = SparkSession.builder.appName("question3sql").getOrCreate()
app.sparkContext.setLogLevel("WARN")

t0 = time.perf_counter()
if args.format == 'csv':
    df1 = app.read.csv(CSV_1, header=True, inferSchema=True)
    df2 = app.read.csv(CSV_2, header=True, inferSchema=True)
else:
    df1 = app.read.parquet(PARQUET_1)
    df2 = app.read.parquet(PARQUET_2)

df1.createOrReplaceTempView("df1")
df2.createOrReplaceTempView("df2")

t1 = time.perf_counter()

sql_query = ("SELECT year, month, crime_total, ranking "
             "FROM ( "
                  "SELECT year, month, crime_total, RANK() OVER (PARTITION BY year ORDER BY year ASC, crime_total DESC) AS ranking "
                  "FROM ("
                      "SELECT YEAR(TO_TIMESTAMP(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) AS year, "
                      "MONTH(TO_TIMESTAMP(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) AS month, "
                      "COUNT(*) AS crime_total "
                      "FROM df1 "
                      "GROUP BY year, month "
                          "UNION "
                      "SELECT YEAR(TO_TIMESTAMP(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) AS year, "
                      "MONTH(TO_TIMESTAMP(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) AS month, "
                      "COUNT(*) AS crime_total "
                      "FROM df2 "
                      "GROUP BY year, month"
                      ") "
              ") "
              "WHERE ranking <= 3")

result = app.sql(sql_query)
result.show()

t2 = time.perf_counter()

if args.format == 'csv':
    print(f"Loading time for the CSV format: {round(t1 - t0, 2)}s")
    print(f"Query time for the CSV format: {round(t2 - t1, 2)}s")
else:
    print(f"Loading time for the Parquet format: {round(t1 - t0, 2)}s")
    print(f"Query time for the Parquet format: {round(t2 - t1, 2)}s")

app.stop()

