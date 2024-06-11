from pyspark.sql import SparkSession

app = SparkSession.builder.appName("question2").getOrCreate()

files = ["Crime_Data_from_2010_to_2019", 
         "Crime_Data_from_2020_to_Present"]

path = 'hdfs://master:9000/home/user/project_data/'
output_path = 'hdfs://master:9000/home/user/project_data/parquet/'

for file_name in files:
    csv_path = path + file_name + ".csv"
    df = app.read.csv(csv_path, header=True, inferSchema=True)
    df.write.parquet(output_path + file_name + ".parquet")

app.stop()

