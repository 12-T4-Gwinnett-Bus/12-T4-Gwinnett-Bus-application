from confluent_kafka import Producer
import random
import time

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Configure the Kafka producer
conf = {'bootstrap.servers': "localhost:9092"}

# Create Producer instance
producer = Producer(conf)

# Function to generate random bus data
def generate_bus_data():
    bus_id = random.randint(1, 100)  # Random bus ID
    speed = round(random.uniform(0, 80), 2)  # Speed in mph (0 to 80 mph)
    latitude = round(random.uniform(33.5, 34.5), 6)  # Latitude within a range (e.g., Atlanta area)
    longitude = round(random.uniform(-84.5, -83.5), 6)  # Longitude within a range (e.g., Atlanta area)
    timestamp = int(time.time())  # Current timestamp in seconds
    return {
        'bus_id': bus_id,
        'speed': speed,
        'latitude': latitude,
        'longitude': longitude,
        'timestamp': timestamp
    }

# Produce 10 messages with random bus data
for _ in range(10):
    bus_data = generate_bus_data()
    message = str(bus_data)  # Convert bus data to string for Kafka message
    producer.produce('bus-data-topic', message.encode('utf-8'), callback=delivery_report)

    # Simulate a delay between data generation (e.g., new data every 5 seconds)
    time.sleep(5)

# Wait for any outstanding messages to be delivered and delivery reports to be received
producer.flush()
