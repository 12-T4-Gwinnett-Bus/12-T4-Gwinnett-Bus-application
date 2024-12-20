import pyodbc
import random
import time
import unittest
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaConsumer
import json

# Kafka connection details
KAFKA_TOPIC = 'bus_data'
KAFKA_SERVER = 'localhost:9092'  # Update this to your Kafka server if different

# SQL Server connection details
server = 'DESKTOP-OUKE0M4'
database = 'BusData'
driver = '{ODBC Driver 17 for SQL Server}'


class BusDataGenerator:
    def __init__(self):
        # Initialize Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )

        # Connect to SQL Server
        connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
        self.conn = pyodbc.connect(connection_string)
        self.cursor = self.conn.cursor()
        print("Connected to SQL Server and Kafka.")

    def generate_random_data(self):
        bus_id = random.randint(1, 100)
        happened_at_time = datetime.now() - timedelta(minutes=random.randint(0, 1000))
        latitude = round(random.uniform(-90, 90), 6)
        longitude = round(random.uniform(-180, 180), 6)
        ecu_speed_mph = random.randint(0, 120)
        gps_speed_mph = random.randint(0, 120)
        heading_degrees = random.randint(0, 360)

        row_data = {
            "bus_id": bus_id,
            "happened_at_time": happened_at_time,
            "latitude": latitude,
            "longitude": longitude,
            "ecu_speed_mph": ecu_speed_mph,
            "gps_speed_mph": gps_speed_mph,
            "heading_degrees": heading_degrees
        }

        # Determine if data is valid or rejected based on speed criteria
        if ecu_speed_mph > 70 or gps_speed_mph > 70:
            self.cursor.execute('''
                INSERT INTO dbo.RejectedEvents (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees))
            print("An invalid record was inserted into RejectedEvents.")
        else:
            self.cursor.execute('''
                INSERT INTO dbo.BusEvent (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees))

            self.cursor.execute('''
                INSERT INTO dbo.ValidEvents (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees))
            print("A valid record was inserted into BusEvent and ValidEvents.")

        # Send the row data to Kafka
        self.producer.send(KAFKA_TOPIC, value=row_data)
        print("Data sent to Kafka:", row_data)

        return row_data

    def fetch_data_from_table(self, table_name, limit=10):
        query = f"SELECT TOP {limit} * FROM {table_name}"
        self.cursor.execute(query)

        # Retrieve column names
        columns = [column[0] for column in self.cursor.description]
        print(" | ".join(columns))  # Print column headers

        # Fetch and print each row
        rows = self.cursor.fetchall()
        for row in rows:
            row_dict = dict(zip(columns, row))
            print(row_dict)  # Print as a dictionary with labels

    def close_connections(self):
        self.conn.close()
        self.producer.flush()  # Ensure all messages are sent
        self.producer.close()
        print("Connection closed.")


# Unit tests
class TestBusDataGenerator(unittest.TestCase):
    def setUp(self):
        self.generator = BusDataGenerator()

    def test_generate_random_data(self):
        
        data = self.generator.generate_random_data()
        
        self.assertIn("bus_id", data)
        
        self.assertIn("ecu_speed_mph", data)
        
        self.assertIn("gps_speed_mph", data)
        
        self.assertGreaterEqual(data["ecu_speed_mph"], 0)
        
        self.assertGreaterEqual(data["gps_speed_mph"], 0)
        
        self.assertLessEqual(data["ecu_speed_mph"], 120)
        
        self.assertLessEqual(data["gps_speed_mph"], 120)

    
    def test_kafka_producer(self):
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        data = self.generator.generate_random_data()
        self.generator.producer.flush()
        consumer.subscribe([KAFKA_TOPIC])
        message = next(consumer)
        self.assertEqual(message.value, data)

    def tearDown(self):
        self.generator.close_connections()


# Main execution
if __name__ == "__main__":
    # Generate and send data for 10 seconds
    generator = BusDataGenerator()
    start_time = time.time()
    while time.time() - start_time < 10:
        generator.generate_random_data()
        generator.conn.commit()
        time.sleep(0.25)

    print("10 seconds of random data generation completed.")

    # Print tables
    print("Data from BusEvent table:")
    generator.fetch_data_from_table("BusEvent")

    print("\nData from ValidEvents table:")
    generator.fetch_data_from_table("ValidEvents")

    print("\nData from RejectedEvents table:")
    generator.fetch_data_from_table("RejectedEvents")

    # Run unit tests
    unittest.main(exit=False)

    # Close connections
    generator.close_connections()
