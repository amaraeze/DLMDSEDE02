from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Jolly',
    'start_date': datetime(2024, 10, 11, 12, 00)
}

def get_data():
    import requests

    # Fetch data from the NYC OpenData API
    res = requests.get("https://data.cityofnewyork.us/resource/m6nq-qud6.json")
    res.raise_for_status()  # Ensure the request was successful
    data = res.json()
    
    # For streaming purposes, we'll simulate streaming by taking one item at a time
    # Modify this to stream multiple items if needed
    return data[0] if data else None

def format_data(record):
    # Assuming relevant fields like pickup and drop-off info
    data = {}
    data['trip_id'] = record.get('trip_id', 'N/A')  # Example field; update based on API fields
    data['pickup_datetime'] = record.get('pickup_datetime', 'N/A')
    data['dropoff_datetime'] = record.get('dropoff_datetime', 'N/A')
    data['passenger_count'] = record.get('passenger_count', 'N/A')
    data['trip_distance'] = record.get('trip_distance', 'N/A')
    data['fare_amount'] = record.get('fare_amount', 'N/A')
    
    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

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
            continue

with DAG('nyc_trip_data_streaming',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_nyc_api',
        python_callable=stream_data
    )

