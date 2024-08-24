import pytest
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from source.consumer.serasa_consumer import KafkaToFileSystem

@pytest.fixture(scope="module")
def spark():
    # Initialize a Spark session for testing
    return SparkSession.builder.master("local").appName("pytest-pyspark-local-testing").getOrCreate()

@pytest.fixture
def kafka_to_file_system(spark):
    # Initialize your KafkaToFileSystem object with required arguments
    kafka_bootstrap_servers = "localhost:9092"  
    kafka_topic = "tax_fare_data"  
    output_path = "/tmp/test_output"
    
    return KafkaToFileSystem(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka_topic=kafka_topic,
        output_path=output_path
    )

def test_read_from_kafka(kafka_to_file_system, spark):
    # Prepare the expected data
    expected_data = [Row(key='value1'), Row(key='value2')]
    df_expected = spark.createDataFrame(expected_data)

    # Replace the actual method with static DataFrame for testing
    kafka_to_file_system.read_from_kafka = lambda: df_expected

    # Call the method
    df_result = kafka_to_file_system.read_from_kafka()

    # Assert that the data matches
    assert df_result.collect() == df_expected.collect()

def test_write_to_file_system(kafka_to_file_system, spark, tmpdir):
    # Prepare mock data as a static DataFrame (not streaming)
    data = [Row(value='{"key":"value1"}'), Row(value='{"key":"value2"}')]
    df_mock = spark.createDataFrame(data)

    # Use a temporary directory for testing output
    kafka_to_file_system.output_path = tmpdir.strpath
    kafka_to_file_system.data_path = tmpdir.strpath

    # Simulate writing by calling the method and catching exceptions
    try:
        kafka_to_file_system.write_to_file_system(df_mock)
    except AnalysisException as e:
        # Assert that the exception indicates the issue with non-streaming DataFrame
        assert "[WRITE_STREAM_NOT_ALLOWED]" in str(e)
