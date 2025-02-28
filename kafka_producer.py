from kafka import KafkaProducer
import csv
import time

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'netflix-de-proj' 

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

# Read CSV and Send Data to Kafka
with open('netflix_titles.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        message = f"{row['show_id']},{row['title']},{row['director']},{row['release_year']},{row['type']}"
        producer.send(TOPIC_NAME, message.encode('utf-8'))
        print(f"Sent: {message}")
        time.sleep(1)  # Simulate streaming data

producer.close()
