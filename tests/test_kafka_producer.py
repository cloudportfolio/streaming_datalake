import pytest
from unittest.mock import patch, MagicMock
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from source.producer.kafka_producer import app


@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

@patch('source.producer.kafka_producer.producer')  # Mock the Producer instance
def test_ingest_data_success(mock_producer, client):
    # Mock the produce and flush methods
    mock_producer.produce = MagicMock()
    mock_producer.flush = MagicMock()

    # Prepare the test data
    test_data = [
        {"key": 1, "fare_amount": 10.0, "pickup_datetime": "2023-08-24T12:00:00Z"},
        {"key": 2, "fare_amount": 15.0, "pickup_datetime": "2023-08-24T12:05:00Z"}
    ]

    # Send a POST request to the /ingest endpoint
    response = client.post('/ingest', json=test_data)

    # Assert the response status code and message
    assert response.status_code == 200
    assert response.json == {"status": "success", "message": "Data ingested successfully"}

    # Assert the producer's produce method was called twice (once for each record)
    assert mock_producer.produce.call_count == len(test_data)

@patch('source.producer.kafka_producer.producer')
def test_ingest_data_failure(mock_producer, client):
    # Mock the produce method to raise an Exception
    mock_producer.produce.side_effect = Exception("Kafka error")

    test_data = [{"key": 1, "fare_amount": 10.0, "pickup_datetime": "2023-08-24T12:00:00Z"}]

    response = client.post('/ingest', json=test_data)

    assert response.status_code == 500
    assert "Failed to send data to Kafka" in response.json['error']

@patch('source.producer.kafka_producer.producer')
def test_ingest_data_invalid_input(mock_producer, client):
    # Send a POST request with invalid data (not a list)
    response = client.post('/ingest', json={"key": 1, "fare_amount": 10.0})

    assert response.status_code == 400
    assert response.json == {"error": "Data should be a list of records"}

    # Ensure the producer's produce method was never called
    mock_producer.produce.assert_not_called()
