from kafka import KafkaProducer
import yfinance as yf
import json
import time
from datetime import datetime

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Stocks to track
stocks = ['AAPL', 'TSLA', 'MSFT', 'GOOGL']

print("Starting stock producer...")

while True:
    for stock in stocks:
        try:
            ticker = yf.Ticker(stock)

            # Get latest market data
            data = ticker.history(period='1d', interval='1m')

            latest = data.tail(1)

            stock_data = {
                "symbol": stock,
                "price": float(latest['Close'].iloc[0]),
                "volume": int(latest['Volume'].iloc[0]),
                "timestamp": datetime.now().isoformat()
            }

            # Send to Kafka
            producer.send('stock-data', value=stock_data)
            producer.flush()

            print(f"Sent: {stock_data}")

        except Exception as e:
            print(f"Error for {stock}: {e}")

    time.sleep(10)