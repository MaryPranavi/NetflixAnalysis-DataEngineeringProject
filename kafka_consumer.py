from kafka import KafkaConsumer

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'netflix-de-proj' 

# Initialize Kafka Consumer
consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=KAFKA_BROKER, auto_offset_reset='earliest')

print("Listening for messages...")
for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")
