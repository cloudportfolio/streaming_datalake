from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, lit
import logging
import os
from datetime import datetime, timedelta

yesterday = datetime.now() - timedelta(days=0)
# Format yesterday's date in yyyy-mm-dd format
yesterday_date = yesterday.strftime('%Y-%m-%d')

class SilverToGold:
    def __init__(self, silver_data_path, gold_data_path):
        self.silver_data_path = f"{silver_data_path}/date={yesterday_date}"
        self.gold_data_path = gold_data_path
        self.spark = SparkSession\
                     .builder\
                     .appName("RawToSilver")\
                     .config("spark.ui.port", "4041") \
                     .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
                     .getOrCreate()
        self.logger = logging.getLogger('SilverToGold')
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def read_silver_data(self):
        self.logger.info("Reading Silver data")
        try:
            df = self.spark.read.parquet(self.silver_data_path)
            return df
        except Exception as e:
            self.logger.error(f"Error reading Silver data: {e}")
            raise

    def build_gold_layer(self, df):
        self.logger.info("Building Gold layer")
        try:
            df_gold = df.withColumn("date", lit(yesterday_date))
            return df_gold
        except Exception as e:
            self.logger.error(f"Error building Gold layer: {e}")
            raise

    def write_gold_data(self, df):
        self.logger.info("Writing Gold data")
        try:
            df.coalesce(1)\
              .write\
              .partitionBy("date")\
              .mode("overwrite")\
              .parquet(self.gold_data_path, compression="snappy")
        except Exception as e:
            self.logger.error(f"Error writing Gold data: {e}")
            raise

    def run(self):
        df_silver = self.read_silver_data()
        df_gold = self.build_gold_layer(df_silver)
        self.write_gold_data(df_gold)

if __name__ == "__main__":
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    data_path = os.path.join(project_root, "data")
    print(data_path)

    silver_data_path = f"{data_path}/silver"
    gold_data_path = f"{data_path}/gold"
    processor = SilverToGold(silver_data_path, gold_data_path)
    processor.run()
