import sys
import os
import json
import threading
from pymongo import MongoClient
from datetime import datetime
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer



# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': '<bootstrap_server>',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '<api_key>',
    'sasl.password': '<api_secret>',
    'group.id': '<group_id>',
    'auto.offset.reset': 'latest'
}

conn_string = "<connection_string>"
Mongodb_client = MongoClient(conn_string)
db = Mongodb_client["gds_db"]
collection = db["logistics_data"]

# To handle serialization of datetime objects,defining a custom encoder.
def datetime_encoder(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()

# 'group.id': 'group1',

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': '<schema_registry_url>',
  'basic.auth.user.info': '{}:{}'.format('<api_key>', '<api_secret>')
})

# Fetch the latest Avro schema for the value
subject_name = 'logistics-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
    # 'enable.auto.commit': True,
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})

# Subscribe to the 'retail_data' topic
consumer.subscribe(['logistics'])



# Define the data_validation_check function
def data_validation_check(value):
    # Add your data validation logic here
    date_format="%Y-%m-%d"
    if value["BookingID_Date"] == "NULL":
        value["BookingID_Date"] = "value not known"
    if value["trip_start_date"] == "NULL":
        value["trip_start_date"] = "value not known"           
    if value["trip_end_date"] == "NULL":
        value["trip_end_date"] = "value not known"
    if not isinstance(value["Curr_lat"], float):
        value["Curr_lat"] = float(value["Curr_lat"])
    if not isinstance(value["Curr_lon"], float):
        value["Curr_lon"] = float(value["Curr_lon"])
    if not isinstance(value["Minimum_kms_to_be_covered_in_a_day"], float):
        value["Minimum_kms_to_be_covered_in_a_day"] = float(value["Minimum_kms_to_be_covered_in_a_day"])
    if not isinstance(value["TRANSPORTATION_DISTANCE_IN_KM"], float):
        value["TRANSPORTATION_DISTANCE_IN_KM"] = float(value["TRANSPORTATION_DISTANCE_IN_KM"])
    
    return value

try:
    while True:
        msg = consumer.poll(1.0)  # How many seconds to wait for a message

        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue
        print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))
        value = msg.value()
        value = data_validation_check(value)
        existing_document = collection.find_one({'BookingID': value['BookingID']})

        if existing_document:
            print(f"Document with bookingID '{value['BookingID']}' already exists. Skipping insertion.")
        else:
            # Insert data into MongoDB
            collection.insert_one(value)
            print("Inserted message into MongoDB:", value)

except KeyboardInterrupt:
    pass
finally:
    consumer.commit()
    consumer.close()
    Mongodb_client.close()