import time
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from utils import *

parser = argparse.ArgumentParser()
parser.add_argument("-e", "--explain", action="store_true")

args = parser.parse_args()

app = SparkSession.builder.appName("query_3_df").config("spark.executor.memory", "2g").getOrCreate()
app.sparkContext.setLogLevel("WARN")

# Remove the dollar sign and comma from the income string, then convert to int
def income_to_int(s: str) -> int:
    # dollar sign is always the first character
    s = s[1:]
    # remove comma (thousands seperator)
    s = s.replace(",", "")    
    return int(s)

udf = F.UserDefinedFunction(income_to_int, T.IntegerType())

t0 = time.perf_counter()

df1 = app.read.parquet(PARQUET_1)
df2 = app.read.parquet(PARQUET_2)

df = df1.union(df2)
df = (df.filter(F.col("Vict Descent").isNotNull())
        .filter(F.substring(F.col("DATE OCC"), 7, 4) == "2015")       
        .select(["LAT", "LON", "Vict Descent"])
        .persist())

# Replace victim descent codes with descriptions
df = df.replace(DESCENT_DICT, subset=["Vict Descent"])

# change column name to match with the one in the revgeocoding data set
income = app.read.csv(INCOME, header=True, inferSchema=True) 
income = income.withColumn("Income", udf("Estimated Median Income"))

revgeo = app.read.csv(REV_GEO, header=True, inferSchema=True) \
    .withColumn("Zip Code", F.substring(F.col("ZIPcode"), 0, 5)) \
    .dropDuplicates(["LAT", "LON"])

JOIN_STRAT = "broadcast"

print("Setting hint: {}".format(JOIN_STRAT))

df = df.hint(JOIN_STRAT)

df = df.join(revgeo.hint(JOIN_STRAT), ["LAT", "LON"]).select(["Zip Code", "Vict Descent"])
df = df.join(income.hint(JOIN_STRAT), "Zip Code")

# Find highest and lowest income values
highest = df.select("Income").distinct().orderBy("Income", ascending=False).limit(3).collect()
highest_vals = [row["Income"] for row in highest]
lowest = df.select("Income").distinct().orderBy("Income", ascending=True).limit(3).collect()
lowest_vals = [row["Income"] for row in lowest]

if args.explain:
    print("Physical plan after the two joins (revgeo and income):")
    df.explain()

highest_df = df.filter(df.Income.isin(highest_vals))
lowest_df = df.filter(df.Income.isin(lowest_vals))

lowest_df = lowest_df.groupBy("Vict Descent").agg(F.count("*").alias("Total Victims")) \
    .orderBy("Total Victims", ascending=False)

highest_df = highest_df.groupBy("Vict Descent").agg(F.count("*").alias("Total Victims")) \
    .orderBy("Total Victims", ascending=False)

results_highest = highest_df.collect()
results_lowest = lowest_df.collect()
t1 = time.perf_counter() 
print("[Query 3 - Dataframes] Done in {:.2f}s".format(t1-t0))
print("Lowest income areas:")
print("Victim Descent, Total Victims")
for row in results_lowest:
    print("{},{}".format(row["Vict Descent"], row["Total Victims"]))

print("\nHighest income areas:")
print("Victim Descent, Total Victims")
for row in results_highest:
    print("{},{}".format(row["Vict Descent"], row["Total Victims"]))

