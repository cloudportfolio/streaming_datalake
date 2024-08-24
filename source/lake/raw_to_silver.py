from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import logging
import os
from pyspark.sql.functions import lit
from datetime import datetime, timedelta
yesterday_date = datetime.now() - timedelta(days=0)
yesterday_date = yesterday_date.strftime('%Y-%m-%d')
class RawToSilver:
    def __init__(self, raw_data_path, silver_data_path):
        self.raw_data_path = f"{raw_data_path}/date={yesterday_date}"
        self.silver_data_path = silver_data_path
        self.spark = SparkSession\
                     .builder\
                     .appName("RawToSilver")\
                     .config("spark.ui.port", "4041") \
                     .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
                     .getOrCreate()
        self.logger = logging.getLogger('RawToSilver')
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # Define the schema for the JSON data
        self.schema = StructType([
            StructField("key", StringType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("pickup_longitude", DoubleType(), True),
            StructField("pickup_latitude", DoubleType(), True),
            StructField("dropoff_longitude", DoubleType(), True),
            StructField("dropoff_latitude", DoubleType(), True),
            StructField("passenger_count", IntegerType(), True)
        ])

    def read_raw_data(self):
        self.logger.info("Reading raw data")
        try:
            df = self.spark.read.schema(self.schema).json(self.raw_data_path)
            df_pickup_date = df.withColumn("date", lit(yesterday_date))
            return df_pickup_date
        except Exception as e:
            self.logger.error(f"Error reading raw data: {e}")
            raise

    def build_silver_layer(self, df):
        self.logger.info("Building Silver layer")
        try:
            df_silver = df.dropDuplicates()
            return df_silver
        except Exception as e:
            self.logger.error(f"Error building Silver layer: {e}")
            raise

    def write_silver_data(self, df):
        self.logger.info("Writing Silver data")
        try:
            df.coalesce(1)\
              .write \
              .partitionBy("date")\
              .mode("overwrite")\
              .parquet(self.silver_data_path, compression="snappy")
            
        except Exception as e:
            self.logger.error(f"Error writing Silver data: {e}")
            raise

    def run(self):
        df_raw = self.read_raw_data()
        df_silver = self.build_silver_layer(df_raw)
        self.write_silver_data(df_silver)

if __name__ == "__main__":
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    data_path = os.path.join(project_root, "data")
    print(data_path)

    raw_data_path = f"{data_path}/raw"
    silver_data_path = f"{data_path}/silver"
    processor = RawToSilver(raw_data_path, silver_data_path)
    processor.run()
