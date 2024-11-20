from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
from kafka import KafkaProducer
import time
import logging

default_args = {
    'owner': 'Jolly',
    'start_date': datetime(2024, 10, 11, 12, 0),
}

def get_data():
    # Fetch data from the NYC OpenData API
    res = requests.get("https://data.cityofnewyork.us/resource/m6nq-qud6.json")
    res.raise_for_status()  # Ensure the request was successful
    data = res.json()
    return data[0] if data else None

def format_data(res):
    # Format the API
    data = {}
    data['vendor_id'] = res.get('VendorID', None)
    data['pickup_datetime'] = res.get('tpep_pickup_datetime', None)
    data['dropoff_datetime'] = res.get('tpep_dropoff_datetime', None)
    data['passenger_count'] = res.get('passenger_count', 0)
    data['trip_distance'] = res.get('trip_distance', 0.0)
    data['rate_code_id'] = res.get('RatecodeID', None)
    data['store_and_fwd_flag'] = res.get('store_and_fwd_flag', None)
    data['pu_location_id'] = res.get('PULocationID', None)
    data['do_location_id'] = res.get('DOLocationID', None)
    data['payment_type'] = res.get('payment_type', None)
    data['fare_amount'] = res.get('fare_amount', 0.0)
    data['extra'] = res.get('extra', 0.0)
    data['mta_tax'] = res.get('mta_tax', 0.0)
    data['tip_amount'] = res.get('tip_amount', 0.0)
    data['tolls_amount'] = res.get('tolls_amount', 0.0)
    data['improvement_surcharge'] = res.get('improvement_surcharge', 0.0)
    data['total_amount'] = res.get('total_amount', 0.0)
    return data

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:  # Run for 1 minute
            break

        try:
            record = get_data()
            if not record:
                logging.warning("No data fetched; terminating stream.")
                break

            formatted_data = format_data(record)
            producer.send('nyc_trip_data', json.dumps(formatted_data).encode('utf-8'))
            logging.info("Data sent to Kafka topic.")
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            time.sleep(5)  # Avoid busy-waiting if an error occurs
            continue

with DAG(
    'nyc_trip_data_streaming',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_nyc_api',
        python_callable=stream_data,
    )
