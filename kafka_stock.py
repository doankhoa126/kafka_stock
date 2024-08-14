from flask import Flask, jsonify
from flask_socketio import SocketIO
from confluent_kafka import Producer, Consumer
import requests
import json
import threading
import time

app = Flask(__name__)
socketio = SocketIO(app)

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-12576z.us-west2.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'D7ANG42AO4HC4LGS',
    'sasl.password': 'qSBWZ5UuR0o4IjEyBLPy8t+xBP0Avp7dnuXDxSQfcT1IevnK0/WUHD9pE0NcvLka',
    'group.id': 'python-group-1',
    'auto.offset.reset': 'earliest',
}

topic = "traddingData"

# Create Kafka Consumer
consumer = Consumer(kafka_config)
consumer.subscribe([topic])

def fetch_data_from_api():
    url = "https://stocktraders.vn/service/data/getTotalTradeReal"
    payload = {"TotalTradeRealRequest": {"account": "StockTraders"}}
    headers = {'Content-Type': 'application/json'}

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 200 or response.status_code == 201:
        return response.json()
    else:
        return None

def produce():
    producer = Producer(kafka_config)
    while True:
        data = fetch_data_from_api()
        if data:
            value = json.dumps(data)
            key = "data_key"
            producer.produce(topic, key=key, value=value)
            producer.flush()
            
        time.sleep(10)

def consume():
    while True:
        msg = consumer.poll(1.0)  # Poll for messages from Kafka
        if msg is not None and not msg.error():
            value = msg.value().decode("utf-8") if msg.value() else None
            if value:
                data = json.loads(value)
                print(f"Received data from topic {topic}: value = {data}")
                socketio.emit('new_data', data)  # Emit to WebSocket clients
        elif msg is not None and msg.error():
            print(f"Consumer error: {msg.error()}")

# Run the consume function in a separate thread
consumer_thread = threading.Thread(target=consume)
consumer_thread.daemon = True
consumer_thread.start()

@app.route('/data')
def index():
    return jsonify({"message": "Server is running"})

if __name__ == "__main__":
    # Start a thread for the producer
    producer_thread = threading.Thread(target=produce)
    producer_thread.daemon = True
    producer_thread.start()

    # Run the Flask server with WebSocket support
    socketio.run(app, host='0.0.0.0', port=5003)
