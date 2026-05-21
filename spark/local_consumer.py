import json
import os
import sys
import time
from kafka import KafkaConsumer

# Initialize the real-time stream subscriber
print("Connecting to stock-data Kafka stream...")
try:
    consumer = KafkaConsumer(
        'stock-data',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
except Exception as e:
    print(f"Connection Failed. Ensure your Kafka broker is accessible: {e}")
    sys.exit(1)

os.system('cls' if os.name == 'nt' else 'clear')
print("=== LIVE STOCK MARKET STREAM ENGINE ===")
print("Listening for real-time stock ticks from producer...\n")

# Process the real-time data dataframes in-memory
try:
    for message in consumer:
        data = message.value
        
        # Cleanly extract fields
        symbol = data.get("symbol", "N/A")
        price = data.get("price", 0.0)
        volume = data.get("volume", 0)
        timestamp = data.get("timestamp", "N/A")
        
        # Print structured console stream dashboard row
        print(f"[{timestamp}] TICKER: {symbol:<5} | PRICE: ${price:<7.2f} | VOLUME: {volume:<6}")
        
except KeyboardInterrupt:
    print("\nStopping stream consumer safely...")