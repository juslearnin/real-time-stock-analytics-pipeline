import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Initialize Kafka Producer
# Initialize Kafka Producer with an explicit API version fallback mapping
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    api_version=(2, 5, 0),  # Forces a stable, universally recognized handshake protocol
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Complete list of 21 tickers
tickers = [
    'AAPL', 'TSLA', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'JPM', 'V', 'DIS', 
    'RELIANCE.NS', 'TCS.NS', 'INFY.NS', 'HDFCBANK.NS', 'ICICIBANK.NS', 'KOTAKBANK.NS', 
    'LT.NS', 'SBIN.NS', 'AXISBANK.NS', 'BAJFINANCE.NS', 'HINDUNILVR.NS'
]

print("Starting stock market live simulation feed...")

try:
    while True:
        for ticker in tickers:
            # Generate clean mock market tracking payloads
            data = {
                "symbol": ticker.strip(),  # Force clean string inputs
                "price": round(random.uniform(100.0, 1500.0), 2),
                "volume": random.randint(1000, 50000),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            producer.send('stock-data', value=data)
            print(f"Sent: {data['symbol']} -> ${data['price']}")
            time.sleep(0.2)  # Controlled streaming pacing
            
except KeyboardInterrupt:
    print("\nStopping data generation feed safely...")
    producer.close()