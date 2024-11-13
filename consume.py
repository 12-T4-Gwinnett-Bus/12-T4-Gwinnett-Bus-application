from confluent_kafka import Consumer, KafkaException
import json

# Kafka consumer configuration
conf = {
    'bootstrap.servers': "localhost:9092",  # Same server as the producer
    'group.id': "bus-monitoring-group",     # Consumer group ID
    'auto.offset.reset': 'earliest'         # Start reading at the earliest offset
}

# Create a Kafka consumer instance
consumer = Consumer(conf)

# Subscribe to the 'bus-data-topic'
consumer.subscribe(['bus-data-topic'])

# Function to process bus data
def process_bus_data(data):
    bus_id = data['bus_id']
    speed = data['speed']
    latitude = data['latitude']
    longitude = data['longitude']
    timestamp = data['timestamp']

    print(f"Processing bus data:")
    print(f"  Bus ID: {bus_id}")
    print(f"  Speed: {speed} mph")
    print(f"  Location: ({latitude}, {longitude})")
    print(f"  Timestamp: {timestamp}")
    # Further processing or saving to a database can be done here

# Consume messages from Kafka
try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll for new messages with a 1-second timeout

        if msg is None:
            continue  # No new message
        if msg.error():
            raise KafkaException(msg.error())

        # Deserialize message from JSON (assuming producer sent the message as a stringified dictionary)
        bus_data_str = msg.value().decode('utf-8')  # Decode message
        bus_data = eval(bus_data_str)  # Convert string to Python dictionary (you could also use json.loads if sending in JSON format)

        # Process the bus data
        process_bus_data(bus_data)

except KeyboardInterrupt:
    print("Consumer stopped.")

finally:
    # Close down the consumer cleanly
    consumer.close()
