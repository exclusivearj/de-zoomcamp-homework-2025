import csv
import json
from kafka import KafkaProducer
from time import time

fields_to_read = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
]

TOPIC_NAME = 'green-trips'
# TOPIC_NAME = 'test-topic'

def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    csv_file = 'data/green_tripdata_2019-10.csv'  # change to your CSV file path if needed
    # csv_file = 'data/head.csv'  # change to your CSV file path if needed

    t0 = time() # Start time
    print(f"Start time: {t0}")

    total_messages = 0
    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        for row in reader:
            # Each row will be a dictionary keyed by the CSV headers
            # print(f"Input record: [{row}")
            message_to_send = {}
            for field in fields_to_read:
                # Add each field to a new dictionary
                message_to_send[field] = row.get(field)
            # print(f"Sending message: [{message_to_send}")
            total_messages += 1
            print(f"Total messages sent: {total_messages}")
            # Send data to Kafka topic
            producer.send(TOPIC_NAME, value=message_to_send)

    # Make sure any remaining messages are delivered
    producer.flush()
    producer.close()
    t1 = time() # End time
    print(f"End time: {t1}")
    took = t1 - t0
    print(f"Time taken: {took}")

if __name__ == "__main__":
    main()

# Time taken = 34.57912492752075