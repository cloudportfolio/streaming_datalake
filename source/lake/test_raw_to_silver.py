import pytest
import tempfile
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, TimestampType, IntegerType
from pyspark.sql.functions import lit
from datetime import datetime, timedelta
from raw_to_silver import RawToSilver  # Replace 'your_module' with the actual module name
import pytz
# Constants
yesterday_date = "2024-08-24"

@pytest.fixture
def setup_temp_dirs():
    # Create temporary directories for raw and silver data
    raw_dir = tempfile.mkdtemp()
    silver_dir = "/Users/patricianati/dev/serasa/source/data/silver/date=2024-08-24"
    
    timestamp_str = "2009-06-15 17:26:21 UTC"
    parsed_datetime = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S %Z")
    utc_timezone = pytz.timezone('UTC')
    localized_datetime = utc_timezone.localize(parsed_datetime)
    timestamp_for_pyspark = localized_datetime.replace(tzinfo=None)
    # Create a subdirectory for the date partition in the raw data directory
    
    #raw_date_dir = f"{raw_dir}/date={yesterday_date}"
    #shutil.os.makedirs(raw_date_dir)

    # Create example raw data in JSON format
    spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
    data = [
        ("1509-05-15 17:26:21.0000001", 4.5, timestamp_for_pyspark, -73.844311, 40.721319, -73.84161, 40.712278, 1),
    ]
    
    schema = StructType([
        StructField("key", StringType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("passenger_count", IntegerType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    #df.withColumn("date", lit(yesterday_date)).write.mode("overwrite").json(raw_date_dir)
    df.withColumn("date", lit(yesterday_date)).coalesce(1)\
              .write \
              .partitionBy("date")\
              .mode("overwrite")\
              .parquet(raw_dir)
    yield raw_dir, silver_dir
    
    # Cleanup
    shutil.rmtree(raw_dir)
    shutil.rmtree(silver_dir)

def test_raw_to_silver(setup_temp_dirs):
    raw_data_path, silver_data_path = setup_temp_dirs

    # Create an instance of RawToSilver
    processor = RawToSilver(raw_data_path, silver_data_path)
    
    # Run the processor
    processor.run()

    # Verify the Silver layer output
    spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
    silver_df = spark.read.parquet(silver_data_path)
    
    # Verify the data in the Silver layer
    assert silver_df.count() == 1
    assert silver_df.columns == ['key', 'fare_amount', 'pickup_datetime', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude', 'passenger_count', 'date']
    
    rows = silver_df.collect()
    assert len(rows) == 1
    
    