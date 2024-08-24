from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_format
import logging
import os

class KafkaToFileSystem:
    def __init__(self, kafka_bootstrap_servers, kafka_topic, output_path):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.output_path = output_path
        self.project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        self.data_path = os.path.join(self.project_root, f"data/{output_path}")
        print(self.data_path)

        # Set up Spark session
        self.spark = SparkSession.builder \
            .appName("KafkaToFileSystem") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
            .getOrCreate()

        # Set up logger
        self.logger = logging.getLogger('KafkaToFileSystem')
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def read_from_kafka(self):
        self.logger.info("Reading data from Kafka")
        try:
            # Read data from Kafka
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", self.kafka_topic) \
                .option("startingOffsets", "earliest")\
                .load()

            # Extract the value column and convert it to string
            df = df.selectExpr("CAST(value AS STRING)")

            return df
        except Exception as e:
            self.logger.error(f"Error reading from Kafka: {e}")
            raise

    def write_to_file_system(self, df):
        self.logger.info("Writing data to file system")
        try:
            # Add a partition column based on the current date
            df = df.withColumn("date", date_format(current_date(), "yyyy-MM-dd"))

            # Write data to file system as JSON files with partitioning
            query = df.writeStream \
                .format("json") \
                .option("checkpointLocation", f"{self.project_root}/checkpoints/raw") \
                .trigger(processingTime="10 second") \
                .option("maxFilesPerTrigger", 1) \
                .partitionBy("date") \
                .outputMode("append") \
                .start(self.data_path)

            query.awaitTermination()
        except Exception as e:
            self.logger.error(f"Error writing to file system: {e}")
            raise

    def run(self):
        df = self.read_from_kafka()
        self.write_to_file_system(df)

if __name__ == "__main__":
    # Replace these values with your actual Kafka bootstrap servers, topic, and output path
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "taxi_fare_data"
    output_path = "raw"

    kafka_to_file_system = KafkaToFileSystem(kafka_bootstrap_servers, kafka_topic, output_path)
    kafka_to_file_system.run()
