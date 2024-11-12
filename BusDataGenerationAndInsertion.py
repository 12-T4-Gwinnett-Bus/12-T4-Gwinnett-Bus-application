import pyodbc
import random
import time
from datetime import datetime, timedelta

# Connection details
server = 'DESKTOP-OUKE0M4'
database = 'BusData'
driver = '{ODBC Driver 17 for SQL Server}'

# Connect to SQL Server
connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()
print("Connected to SQL Server.")


# Generate and insert random data
def generate_random_data():
    bus_id = random.randint(1, 100)  # Random bus ID between 1 and 100
    happened_at_time = datetime.now() - timedelta(minutes=random.randint(0, 1000))  # Random time up to 1000 minutes ago
    latitude = round(random.uniform(-90, 90), 6)  # Latitude within valid range
    longitude = round(random.uniform(-180, 180), 6)  # Longitude within valid range
    ecu_speed_mph = random.randint(0, 120)  # ECU speed between 0 and 120 mph
    gps_speed_mph = random.randint(0, 120)  # GPS speed between 0 and 120 mph
    heading_degrees = random.randint(0, 360)  # Heading between 0 and 360 degrees

    # Validate records by checking speed
    if ecu_speed_mph > 70 or gps_speed_mph > 70:
        # Insert into RejectedEvents if speed exceeds 70
        cursor.execute('''
            INSERT INTO dbo.RejectedEvents (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees))
        print("An invalid record was inserted into RejectedEvents.")
    else:
        # Insert into both BusEvent and ValidEvents if speed is 70 or below
        cursor.execute('''
            INSERT INTO dbo.BusEvent (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees))

        cursor.execute('''
            INSERT INTO dbo.ValidEvents (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (bus_id, happened_at_time, latitude, longitude, ecu_speed_mph, gps_speed_mph, heading_degrees))
        print("A valid record was inserted into BusEvent and ValidEvents.")


# Function to retrieve data from a table
def fetch_data_from_table(table_name, limit=10):
    query = f"SELECT TOP {limit} * FROM {table_name}"
    cursor.execute(query)

    # Retrieve column names from cursor description
    columns = [column[0] for column in cursor.description]
    print(" | ".join(columns))  # Print column headers

    # Fetch and print each row with column names
    rows = cursor.fetchall()
    for row in rows:
        row_dict = dict(zip(columns, row))  # Pair column names with row values
        print(row_dict)  # Print as a dictionary with labels


# Generate data continuously for 10 seconds
start_time = time.time()
while time.time() - start_time < 10:
    generate_random_data()
    conn.commit()
    time.sleep(0.25)  # Sleep for 0.25 seconds before generating the next record

print("10 seconds of random data generation completed.")

# Print tables
print("Data from BusEvent table:")
fetch_data_from_table("BusEvent")

print("\nData from ValidEvents table:")
fetch_data_from_table("ValidEvents")

print("\nData from RejectedEvents table:")
fetch_data_from_table("RejectedEvents")

# Close connection
conn.close()
print("Connection closed.")
