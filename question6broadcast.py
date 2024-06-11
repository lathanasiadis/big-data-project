import time
import csv
from operator import add
from pyspark import SparkFiles
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

def map_func(x):
    # probe hash table
    stations_data = bc.value.get(x[0])
    if stations_data is None:
        return None
    return x + stations_data
    
sc = SparkSession.builder.appName("query_4_broadcast").getOrCreate().sparkContext
sc.setLogLevel("WARN")

# Instead of broadcasting the file and building the hash table on each node,
# spark allows us to broadcast the hash table directly

STATIONS_LOCAL = "/home/user/data/LAPD_Stations.csv"
# Build the hash table
h = {}
with open(STATIONS_LOCAL, "r") as f:
    for i, line in enumerate(f):
        if i == 0:
            # skip csv header
            continue
        items = line.split(",")
        h[int(items[STATIONS_PRECINCT_IDX])] = [items[STATIONS_DIVISION_IDX],
                                           items[STATIONS_X_IDX],
                                           items[STATIONS_Y_IDX]]   

bc = sc.broadcast(h)

crimes = sc.textFile(CSV_1 + "," + CSV_2).mapPartitions(lambda x: csv.reader(x))
# Ignore crimes that don't involve firearms
crimes = crimes.filter(lambda x: x[CRIMES_WEP_USED_IDX] != "" and x[CRIMES_WEP_USED_IDX][0] == "1")
# Drop columns that are no longer needed, convert area code to int
crimes = crimes.map(lambda x: [int(x[CRIMES_AREA_IDX]), x[CRIMES_LAT_IDX], x[CRIMES_LON_IDX]])

crimes = crimes.map(map_func)
results = crimes.collect()

print("First 10 items from the joined RDD:")
print("items are key-value pairs with key: area/precinct code and value: [weapon used code, longitude, latitude, division name, division PD X coord, division PD Y coord]\n")
for item in results[:10]:
    print(item)

