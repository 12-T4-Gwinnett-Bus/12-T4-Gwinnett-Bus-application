import pyodbc
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json

# Kafka connection details
KAFKA_TOPIC = 'bus_data'
KAFKA_SERVER = 'localhost:9092'  # Update this to your Kafka server if different

# SQL Server connection details
server = 'DESKTOP-OUKE0M4'
database = 'BusData'
driver = '{ODBC Driver 17 for SQL Server}'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

# Connect to SQL Server
connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()
print("Connected to SQL Server and Kafka.")


# Generate and insert random data
def generate_random_data():
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
        cursor.execute('''
            INSERT INTO dbo.RejectedEvents (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees))
        print("An invalid record was inserted into RejectedEvents.")
    else:
        cursor.execute('''
            INSERT INTO dbo.BusEvent (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees))

        cursor.execute('''
            INSERT INTO dbo.ValidEvents (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees))
        print("A valid record was inserted into BusEvent and ValidEvents.")

    # Send the row data to Kafka
    producer.send(KAFKA_TOPIC, value=row_data)
    print("Data sent to Kafka:", row_data)


# Function to retrieve and print data from a table
def fetch_data_from_table(table_name, limit=10):
    query = f"SELECT TOP {limit} * FROM {table_name}"
    cursor.execute(query)

    # Retrieve column names
    columns = [column[0] for column in cursor.description]
    print(" | ".join(columns))  # Print column headers

    # Fetch and print each row
    rows = cursor.fetchall()
    for row in rows:
        row_dict = dict(zip(columns, row))
        print(row_dict)  # Print as a dictionary with labels


# Generate and send data for 10 seconds
start_time = time.time()
while time.time() - start_time < 10:
    generate_random_data()
    conn.commit()
    time.sleep(0.25)

print("10 seconds of random data generation completed.")

# Print tables
print("Data from BusEvent table:")
fetch_data_from_table("BusEvent")

print("\nData from ValidEvents table:")
fetch_data_from_table("ValidEvents")

print("\nData from RejectedEvents table:")
fetch_data_from_table("RejectedEvents")

# Close connections
conn.close()
producer.flush()  # Ensure all messages are sent
producer.close()
print("Connection closed.")
