import time
import csv
from operator import add
from pyspark.sql import SparkSession

from utils import *

TIME_OCC_IDX = 3
PREMIS_DESC_IDX = 15

def time_cat(time_occ: str) -> str:
    hour = int(time_occ)
    if hour >= 500 and hour < 1200:
        return "Morning"
    if hour >= 1200 and hour < 1700:
        return "Afternoon"
    if hour >= 1700 and hour < 2100:
        return "Evening"
    return "Night"
    
sc = SparkSession.builder.appName("question4rdd").getOrCreate().sparkContext
sc.setLogLevel("WARN")

t0 = time.perf_counter()

rdd = sc.textFile(CSV_1 + "," + CSV_2)
# required to deal with memory issues
# rdd = rdd.persist() 

t1 = time.perf_counter()

# use csv.reader to deal with csv cells containing commas
# then filter to return crimes that occured on a street
rdd = rdd.mapPartitions(lambda x: csv.reader(x)).filter(
	lambda x: x[PREMIS_DESC_IDX] == "STREET")

rdd = (rdd.map(lambda x: [time_cat(x[TIME_OCC_IDX]), 1])
          .reduceByKey(add)
          .sortBy(lambda x: x[1], ascending=False))

results = rdd.collect()

t2 = time.perf_counter()

print("[Query 2 - RDD] Data loading done in {:.2f}s".format(t1-t0))
print("[Query 2 - RDD] Query done in {:.2f}s".format(t2-t1))
print("Results:\n{}".format(results))

