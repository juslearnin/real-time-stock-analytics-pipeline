from kafka import KafkaConsumer
import json

# Create Kafka consumer
consumer = KafkaConsumer(
    'stock-data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='stock-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for stock market data...\n")

# Continuously listen for messages
for message in consumer:
    data = message.value

    print(f"{data['symbol']} | Price: {data['price']} | Volume: {data['volume']}")