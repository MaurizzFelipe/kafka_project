# You can use this as a backbone to create a consumer to process kafka data

from kafka import KafkaConsumer
import json

# Set your Kafka broker's IP and port here
kafka_broker = 'localhost:9092'

# Set the Kafka topic name to consume data from
kafka_topic = 'topic name'

# Create a KafkaConsumer instance
consumer = KafkaConsumer(kafka_topic,
                         bootstrap_servers=kafka_broker,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Function to process and print data received from Kafka
def process_data():
    for message in consumer:
        data = message.value
        print(data)  # You can process the data here as needed

if __name__ == "__main__":
    process_data()
