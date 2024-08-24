from flask import Flask, request, jsonify
from flask_cors import CORS
from confluent_kafka import Producer
import json
import logging

logging.basicConfig(level=logging.DEBUG)
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Kafka producer configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'compression.type': 'gzip',
    'batch.num.messages': 100,
    'linger.ms': 100,
    'acks': 'all'
}

producer = Producer(kafka_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_messages(topic, data):
    for record in data:
        key = str(record['key'])
        value = json.dumps(record)
        producer.produce(topic, key=key, value=value, callback=delivery_report)
    producer.flush()

@app.route('/ingest', methods=['POST'])
def ingest_data():
    data = request.json
    if not isinstance(data, list):
        return jsonify({"error": "Data should be a list of records"}), 400

    topic = 'taxi_fare_data'
    try:
        produce_messages(topic, data)
    except Exception as e:
        return jsonify({"error": f"Failed to send data to Kafka: {e}"}), 500

    return jsonify({"status": "success", "message": "Data ingested successfully"}), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
