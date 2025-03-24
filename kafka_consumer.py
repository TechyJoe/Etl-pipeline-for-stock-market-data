from confluent_kafka import Consumer
import json
import pandas as pd

# Kafka Consumer Configuration
conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'stock_group', 'auto.offset.reset': 'earliest'}
consumer = Consumer(conf)
consumer.subscribe(['stock_prices'])

while True:
    msg = consumer.poll(1.0)  
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue

    data = json.loads(msg.value().decode('utf-8'))
    df = pd.DataFrame.from_dict(data)  # Convert JSON to DataFrame
    print(df.head())  # Process data
