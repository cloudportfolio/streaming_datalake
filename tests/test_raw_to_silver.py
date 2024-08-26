import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, TimestampType, IntegerType
#from source.lake.raw_to_silver import RawToSilver # Adjust the import based on your project structure
from datetime import datetime, timedelta
import tempfile
import shutil
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from source.lake.raw_to_silver import RawToSilver

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("TestSession") \
        .master("local[1]") \
        .getOrCreate()

@pytest.fixture(scope="module")
def temp_data_paths():
    raw_data_path = tempfile.mkdtemp()
    silver_data_path = tempfile.mkdtemp()

    yield raw_data_path, silver_data_path

    shutil.rmtree(raw_data_path)
    shutil.rmtree(silver_data_path)

@pytest.fixture
def raw_data(spark, temp_data_paths):
    raw_data_path, _ = temp_data_paths
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

    data = [
        ("1", 10.5, datetime.now(), -73.935242, 40.730610, -73.935242, 40.730610, 1),
        ("2", 15.0, datetime.now(), -73.935242, 40.730610, -73.935242, 40.730610, 2)
    ]

    df = spark.createDataFrame(data, schema=schema)
    df.write.mode("overwrite").json(raw_data_path)
    return raw_data_path

def test_read_raw_data(spark, raw_data, temp_data_paths):
    raw_data_path, _ = temp_data_paths
    processor = RawToSilver(raw_data_path, "")
    df = processor.read_raw_data()

    assert df.count() == 2
    assert "date" in df.columns

def test_build_silver_layer(spark, raw_data, temp_data_paths):
    raw_data_path, _ = temp_data_paths
    processor = RawToSilver(raw_data_path, "")
    df_raw = processor.read_raw_data()
    df_silver = processor.build_silver_layer(df_raw)

    assert df_silver.count() == 2

def test_write_silver_data(spark, raw_data, temp_data_paths):
    raw_data_path, silver_data_path = temp_data_paths
    processor = RawToSilver(raw_data_path, silver_data_path)
    df_raw = processor.read_raw_data()
    df_silver = processor.build_silver_layer(df_raw)
    processor.write_silver_data(df_silver)

    assert len(os.listdir(silver_data_path)) > 0  # Check if the directory is not empty
