from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_format
import os
import sys
# Initialize Spark session
spark = SparkSession.builder \
    .appName("ReadAndWriteToConsole") \
    .getOrCreate()

# Define the path to the data
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
data_path = os.path.join(project_root, "data/gold")
print(data_path)

# Read the data from the path
df = spark.read.parquet(f"{data_path}/date=2024-08-26")

# Write the data to the console
df.select("key","fare_amount").show(truncate=False)

# Stop the Spark session
spark.stop()
