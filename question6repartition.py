import time
import csv
from operator import add
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from utils import *

CRIMES_AREA_IDX = 4
CRIMES_WEP_USED_IDX = 16
CRIMES_LAT_IDX = 26
CRIMES_LON_IDX = 27
STATIONS_DIVISION_IDX = 1
STATIONS_PRECINCT_IDX = 3
STATIONS_X_IDX = 4
STATIONS_Y_IDX = 5

def map_func(items):
    from_crimes = []
    from_stations = []
    
    for item in items:
        origin = item[0]
        if origin == "C":
            from_crimes.append(item[1:])
        else:
            from_stations.append(item[1:])   
    new_records = []
    for i in from_crimes:
        for j in from_stations:
            new_records.append(i+j)
    return new_records
 
sc = SparkSession.builder.appName("query_4_repartition").getOrCreate().sparkContext
sc.setLogLevel("WARN")

crimes = sc.textFile(CSV_1 + "," + CSV_2).mapPartitions(lambda x: csv.reader(x))
stations = sc.textFile(LAPD_STATIONS).mapPartitions(lambda x: csv.reader(x))

# Ignore crimes that don't involve firearms
crimes = crimes.filter(lambda x: x[CRIMES_WEP_USED_IDX] != "" and x[CRIMES_WEP_USED_IDX][0] == "1")

# Tag each RDD according to its source, discard irrelevant record columns and
# use the join variable (area/precintct) as the key
crimes = crimes.map(lambda x: (x[CRIMES_AREA_IDX], ["C",
                                                    x[CRIMES_WEP_USED_IDX],
                                                    x[CRIMES_LAT_IDX],
                                                    x[CRIMES_LON_IDX]]))
stations = stations.map(lambda x: (x[STATIONS_PRECINCT_IDX], ["S",
                                                              x[STATIONS_DIVISION_IDX],
                                                              x[STATIONS_X_IDX],
                                                              x[STATIONS_Y_IDX]]))

rdd = crimes.union(stations).groupByKey().flatMapValues(map_func)

results = rdd.collect()

print("First 10 items from the joined RDD:")
print("items are key-value pairs with key: area/precinct code and value: [weapon used code, longitude, latitude, division name, division PD X coord, division PD Y coord]\n")
for item in results[:10]:
    print(item)

