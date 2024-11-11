# #import uuid
# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python import PythonOperator

# default_args = {
#     'owner': 'Jolly',
#     'start_date': datetime(2024, 10, 11, 12, 00)
# }

# def get_data():
#     import requests

#     res = requests.get("https://randomuser.me/api/")
#     res = res.json()
#     res = res['results'][0]
    
#     return res

# def format_data(res):
#     data = {}
#     location = res['location']
#     #data['id'] = uuid.uuid4()
#     data['first_name'] = res['name']['first']
#     data['last_name'] = res['name']['last']
#     data['gender'] = res['gender']
#     data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
#                       f"{location['city']}, {location['state']}, {location['country']}"
#     data['post_code'] = location['postcode']
#     data['email'] = res['email']
#     data['username'] = res['login']['username']
#     data['dob'] = res['dob']['date']
#     data['registered_date'] = res['registered']['date']
#     data['phone'] = res['phone']
#     data['picture'] = res['picture']['medium']

#     return data

# def stream_data():
#     import json
#     from kafka import KafkaProducer
#     import time
#     import logging

    
#     #print(json.dumps(res, indent =3))

#     producer = KafkaProducer(bootstrap_servers =['broker:29092'], max_block_ms=5000)
#     curr_time = time.time()


#     while True:
#         if time.time() > curr_time + 60: #1 minute
#             break
        
#         try:
#             res = get_data()
#             res = format_data(res)
            
#             producer.send('users_created', json.dumps(res).encode('utf-8'))
#         except Exception as e:
#             logging.error(f'An error occured: {e}')
#             continue

# with DAG('user_automation',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:
    
#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )

# #stream_data()
#------------------------------------------------------------------------------------------
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




#--------------------------------------------------------------------------------------------------------------------------------------------------------
##MAIN DATA
# from datetime import datetime
# import pandas as pd 
# from airflow import DAG
# from airflow.operators.python import PythonOperator

# default_args = {
#     'owner': 'Jolly',
#     'start_date': datetime(2024, 10, 11, 12, 00)
# }

# def read_excel_data(file_path):
#     # Read the Yellow Taxi trip data from the Excel file using pandas
#     df = pd.read_csv(file_path)
#     return df

# def format_data(row):
#     # Format the data based on the trip record structure
#     data = {}
#     data['vendor_id'] = row['VendorID']
#     data['pickup_datetime'] = row['tpep_pickup_datetime']
#     data['dropoff_datetime'] = row['tpep_dropoff_datetime']
#     data['passenger_count'] = row['passenger_count']
#     data['trip_distance'] = row['trip_distance']
#     # data['pickup_location'] = {
#     #     'latitude': row['pickup_latitude'],
#     #     'longitude': row['pickup_longitude']
#     # }
#     # data['dropoff_location'] = {
#     #     'latitude': row['dropoff_latitude'],
#     #     'longitude': row['dropoff_longitude']
#     # }
#     data['Rate_codeID'] = row['RatecodeID']
#     data['Store_and_fwd_Flag'] = row['store_and_fwd_flag']
#     data['PU_LocationID'] = row['PULocationID']
#     data['DO_LocationID'] = row['DOLocationID']
#     data['Payment_Type'] = row['payment_type']
#     data['fare_amount'] = row['fare_amount']
#     data['Extra'] = row['extra']
#     data['Mta_tax'] = row['mta_tax']
#     data['tip_amount'] = row['tip_amount']
#     data['tolls_amount'] = row['tolls_amount']
#     data['improvement_surcharge'] = row['improvement_surcharge']
#     data['total_amount'] = row['total_amount']

#     return data

# def stream_data():
#     import json
#     from kafka import KafkaProducer
#     import time
#     import logging
    
#     # Kafka producer setup
#     producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
#     curr_time = time.time()
    
#     # Read the Excel (or CSV) file with taxi trip data
#     file_path = 'C:/Users/Jollybabe/Documents/IU/Data_Engineering_Course/Project/2018_Yellow_Taxi_Trip_Data_20240729.csv'  # Update this path
#     df = read_excel_data(file_path)

#     while True:
#         if time.time() > curr_time + 60:  # Stream for 1 minute
#             break
        
#         try:
#             # Loop through the DataFrame rows and send each taxi trip record to Kafka
#             for index, row in df.iterrows():
#                 res = format_data(row)
#                 producer.send('taxi_trip_data', json.dumps(res).encode('utf-8'))
#                 time.sleep(1)  # Optional: Add delay between records if needed
                
#         except Exception as e:
#             logging.error(f'An error occurred: {e}')
#             continue

# with DAG('taxi_trip_streaming',
#          default_args=default_args,
#          schedule='@daily',
#          catchup=False) as dag:
    
#     streaming_task = PythonOperator(
#         task_id='stream_taxi_trip_data_from_csv',
#         python_callable=stream_data
#     )

# #stream_data()

# #SMALLER DATA
# from datetime import datetime
# import pandas as pd
# from airflow import DAG
# from airflow.operators.python import PythonOperator

# default_args = {
#     'owner': 'Jolly',
#     'start_date': datetime(2024, 10, 11, 12, 00)
# }

# def read_csv_data(file_path):
#     # Read the weather classification data from the CSV file using pandas
#     df = pd.read_csv(file_path)
#     return df

# def format_weather_data(row):
#     # Format the data based on the weather classification structure
#     data = {}
#     data['temperature'] = row['Temperature']
#     data['humidity'] = row['Humidity']
#     data['wind_speed'] = row['Wind Speed']
#     data['precipitation'] = row['Precipitation (%)']
#     data['cloud_cover'] = row['Cloud Cover']
#     data['atmospheric_pressure'] = row['Atmospheric Pressure']
#     data['uv_index'] = row['UV Index']
#     data['season'] = row['Season']
#     data['visibility'] = row['Visibility (km)']
#     data['location'] = row['Location']
#     data['weather_type'] = row['Weather Type']
    
#     return data

# def stream_weather_data():
#     import json
#     from kafka import KafkaProducer
#     import time
#     import logging
    
#     # Kafka producer setup
#     producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
#     curr_time = time.time()
    
#     # Read the CSV file with weather data
#     file_path = 'C:/Users/Jollybabe/Documents/IU/Data_Engineering_Course/Project/weather_classification_data.csv'  # Update this path if necessary
#     df = read_csv_data(file_path)

#     while True:
#         if time.time() > curr_time + 60:  # Stream for 1 minute
#             break
        
#         try:
#             # Loop through the DataFrame rows and send each weather record to Kafka
#             for index, row in df.iterrows():
#                 res = format_weather_data(row)
#                 producer.send('weather_data', json.dumps(res).encode('utf-8'))
#                 time.sleep(1)  # Optional: Add delay between records if needed
                
#         except Exception as e:
#             logging.error(f'An error occurred: {e}')
#             continue

# with DAG('weather_data_streaming',
#          default_args=default_args,
#          schedule='@daily',
#          catchup=False) as dag:
    
#     streaming_task = PythonOperator(
#         task_id='stream_weather_data_from_csv',
#         python_callable=stream_weather_data
#     )
