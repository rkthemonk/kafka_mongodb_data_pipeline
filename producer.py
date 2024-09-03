import json
import time
import pandas as pd
import mysql.connector
from mysql.connector import Error
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer



# function returns Kafka configuration
def get_kafka_config():
    kafka_config = {
        'bootstrap.servers': "<bootstrap_server>",
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': '<username>',
        'sasl.password': '<password>',
        'group.id': '<group_id>',
        'auto.offset.reset': 'earliest'
    }
    return kafka_config


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def fetch_send_data():
    try:
        # Fetching Kafka configuration
        kafka_config = get_kafka_config()

        # Create a Schema Registry client
        schema_registry_client = SchemaRegistryClient({
        'url': '<schema_registry_url>',
        'basic.auth.user.info': '{}:{}'.format('<api_key>', '<api_secret>')
        })

        # Fetch the latest Avro schema for the value
        subject_name = 'logistics-value'
        schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

        # Create Avro Serializer for the value
        key_serializer = StringSerializer('utf_8')
        avro_serializer = AvroSerializer(schema_registry_client, schema_str)

        # Define the SerializingProducer
        producer = SerializingProducer({
            'bootstrap.servers': kafka_config['bootstrap.servers'],
            'security.protocol': kafka_config['security.protocol'],
            'sasl.mechanisms': kafka_config['sasl.mechanisms'],
            'sasl.username': kafka_config['sasl.username'],
            'sasl.password': kafka_config['sasl.password'],
            'key.serializer': key_serializer,  # Key will be serialized as a string
            'value.serializer': avro_serializer  # Value will be serialized as Avro
        })


        # Load the CSV data into a pandas DataFrame
        data = pd.read_csv('logistics_data.csv')
        object_columns = data.select_dtypes(include=['object']).columns
        data[object_columns] = data[object_columns].fillna('unknown value')
        print(data.head(4))

        # Iterate over DataFrame rows and produce to Kafka
        for index, row in data.iterrows():
            # Create a dictionary from the row values
            logistics_data = {
            "GpsProvider": row["GpsProvider"],
            "BookingID": row["BookingID"],
            "Market/Regular ": row["Market/Regular "],
            "BookingID_Date": row["BookingID_Date"],
            "vehicle_no": row["vehicle_no"],
            "Origin_Location": row["Origin_Location"],
            "Destination_Location": row["Destination_Location"],
            "Org_lat_lon": row["Org_lat_lon"],
            "Des_lat_lon": row["Des_lat_lon"],
            "Data_Ping_time": row["Data_Ping_time"],
            "Planned_ETA": row["Planned_ETA"],
            "Current_Location": row["Current_Location"],
            "DestinationLocation": row["DestinationLocation"],
            "actual_eta": row["actual_eta"],
            "Curr_lat": row["Curr_lat"],
            "Curr_lon": row["Curr_lon"],
            "ontime": row["ontime"],
            "delay": row["delay"],
            "OriginLocation_Code": row["OriginLocation_Code"],
            "DestinationLocation_Code": row["DestinationLocation_Code"],
            "trip_start_date": row["trip_start_date"],
            "trip_end_date": row["trip_end_date"],
            "TRANSPORTATION_DISTANCE_IN_KM": row["TRANSPORTATION_DISTANCE_IN_KM"],
            "vehicleType": row["vehicleType"],
            "Minimum_kms_to_be_covered_in_a_day": row["Minimum_kms_to_be_covered_in_a_day"],
            "Driver_Name": row["Driver_Name"],
            "Driver_MobileNo": row["Driver_MobileNo"],
            "customerID": row["customerID"],
            "customerNameCode": row["customerNameCode"],
            "supplierID": row["supplierID"],
            "supplierNameCode": row["supplierNameCode"],
            "Material Shipped": row["Material Shipped"],
            # Add other fields as needed
        }
            # Produce to Kafka
            producer.produce(topic='logistics', key=str(logistics_data['BookingID']), value=logistics_data, on_delivery=delivery_report)
            producer.flush()
            time.sleep(0.5)
            #break
    except Exception as e:
        print(e)

fetch_send_data()