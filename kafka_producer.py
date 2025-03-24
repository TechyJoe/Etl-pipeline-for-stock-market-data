from confluent_kafka import Producer
import yfinance as yf
import json
import time

# Kafka configuration
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

def fetch_and_send(stock_symbol):
    stock = yf.Ticker(stock_symbol)
    data = stock.history(period="1d", interval="1m").to_dict()
    
    message = json.dumps(data)
    producer.produce('stock_prices', key=stock_symbol, value=message)
    producer.flush()

while True:
    fetch_and_send("AAPL")
    time.sleep(60)  # Fetch data every minute
