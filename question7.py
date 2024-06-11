import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from geopy.distance import geodesic
from pyspark.sql.types import FloatType, StructType, StructField
from pyproj import Transformer
from utils import CSV_1, CSV_2, LAPD_STATIONS

def geodesic_dist(lat1, long1, lat2, long2):
    return geodesic((lat1, long1), (lat2, long2)).km

def _coord_transform(x, y):
    transformer = Transformer.from_crs("EPSG:2229", "EPSG:4326")
    return transformer.transform(x, y)

coord_transform = F.udf(_coord_transform, StructType([
                         StructField("lat", FloatType(), False),
                         StructField("long", FloatType(), False)]))

app = SparkSession.builder.appName("question7").getOrCreate()
app.sparkContext.setLogLevel("WARN")

df1 = app.read.csv(CSV_1, header=True, inferSchema=True)
df2 = app.read.csv(CSV_2, header=True, inferSchema=True)
df = df1.union(df2)
# Drop Null Island entries
df = df.filter((df.LAT != 0.0) & (df.LON != 0.0))

df_police = app.read.csv(LAPD_STATIONS, header=True, inferSchema=True) \
    .withColumn("police_lat_long", coord_transform(F.col("x"), F.col("y")))

dist_func = F.udf(geodesic_dist, FloatType())

df = (df.filter(F.col("Weapon Used Cd").rlike(r"1\d{2}"))
        .withColumnRenamed("AREA ", "PREC")
        .join(df_police, on="PREC")
        .withColumn("dist", dist_func(F.col("LAT"), F.col("LON"), F.col("police_lat_long.lat"), F.col("police_lat_long.long")))
        .groupby("DIVISION").agg(F.count("*").alias("Total Incidents"), F.avg("dist").alias("Average Distance"))
        .orderBy("Total Incidents", ascending=False))

df.show()

