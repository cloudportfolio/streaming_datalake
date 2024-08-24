import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from datetime import datetime
import os
from silver_to_gold import SilverToGold  # Adjust the import based on your file structure

@pytest.fixture
def setup_temp_dirs():
    # Create temporary directories for testing
    import tempfile
    silver_data_path = tempfile.mkdtemp()
    gold_data_path = "/Users/patricianati/dev/serasa/source/data/silver/date=2024-08-24"
    yield silver_data_path, gold_data_path

def test_silver_to_gold(setup_temp_dirs):
    silver_data_path, gold_data_path = setup_temp_dirs
    
    # Create a Spark session for testing
    spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

    # Define the schema for the Silver data
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
    
    # Prepare test data
    test_data = [
         ("1509-05-15 17:26:21.0000001", 4.5, timestamp_for_pyspark, -73.844311, 40.721319, -73.84161, 40.712278, 1),
    ]
    
    # Create a DataFrame with the test data
    df = spark.createDataFrame(test_data, schema)
    
    # Write the DataFrame to silver_data_path
    df.write.mode('overwrite').parquet(silver_data_path)
    
    # Create an instance of SilverToGold
    processor = SilverToGold(silver_data_path, gold_data_path)
    
    # Run the processor
    processor.run()
    
    # Read the result DataFrame from the gold_data_path
    result_df = spark.read.parquet(gold_data_path)
    
    # Collect and check results
    results = result_df.collect()
    
    # Verify the results
    assert len(results) == 1  # Adjust based on your expected number of rows
    assert results[0]['key'] == '1'
    assert results[0]['fare_amount'] == 10.5
    assert results[0]['pickup_datetime'] == datetime(2024, 8, 24, 0, 0)
    assert results[0]['pickup_longitude'] == -73.9855
    assert results[0]['pickup_latitude'] == 40.748817
    assert results[0]['dropoff_longitude'] == -73.9855
    assert results[0]['dropoff_latitude'] == 40.748817
    assert results[0]['passenger_count'] == 1
    assert results[0]['date'] == datetime(2024, 8, 24).strftime('%Y-%m-%d')
