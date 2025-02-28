from kafka import KafkaConsumer
import psycopg2
import json

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'netflix-de-proj'

# PostgreSQL Configuration
DB_NAME = 'netflix_data'  # The database you created
USER = 'postgres'    # PostgreSQL username
PASSWORD = '5650'  # PostgreSQL password
HOST = 'localhost'         # Database host

# Initialize Kafka Consumer
consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=KAFKA_BROKER, auto_offset_reset='earliest')

# Initialize PostgreSQL Connection
conn = psycopg2.connect(dbname=DB_NAME, user=USER, password=PASSWORD, host=HOST)
cursor = conn.cursor()

# Listen for messages and insert into PostgreSQL
for message in consumer:
    # Decode and split the CSV data
    data = message.value.decode('utf-8').split(',')
    
    # Insert data into PostgreSQL table
    if len(data) == 5:  # Check if the split message has 5 fields
        cursor.execute("""
            INSERT INTO netflix_data (show_id, title, director, release_year, type)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (show_id) DO NOTHING
        """, (data[0], data[1], data[2], int(data[3]), data[4]))
        
        conn.commit()
        print(f"Inserted: {data}")

# Close PostgreSQL connection
cursor.close()
conn.close()
