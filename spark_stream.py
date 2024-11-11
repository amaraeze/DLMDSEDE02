# import logging

# from cassandra.cluster import Cluster
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType


# def create_keyspace(session):
#     session.execute("""
#         CREATE KEYSPACE IF NOT EXISTS spark_streams
#         WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
#     """)

#     print("Keyspace created successfully!")


# def create_table(session):
#     session.execute("""
#     CREATE TABLE IF NOT EXISTS spark_streams.created_users (
#         id UUID PRIMARY KEY,
#         first_name TEXT,
#         last_name TEXT,
#         gender TEXT,
#         address TEXT,
#         post_code TEXT,
#         email TEXT,
#         username TEXT,
#         registered_date TEXT,
#         phone TEXT,
#         picture TEXT);
#     """)

#     print("Table created successfully!")


# def insert_data(session, **kwargs):
#     print("inserting data...")

#     user_id = kwargs.get('id')
#     first_name = kwargs.get('first_name')
#     last_name = kwargs.get('last_name')
#     gender = kwargs.get('gender')
#     address = kwargs.get('address')
#     postcode = kwargs.get('post_code')
#     email = kwargs.get('email')
#     username = kwargs.get('username')
#     dob = kwargs.get('dob')
#     registered_date = kwargs.get('registered_date')
#     phone = kwargs.get('phone')
#     picture = kwargs.get('picture')

#     try:
#         session.execute("""
#             INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
#                 post_code, email, username, dob, registered_date, phone, picture)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """, (user_id, first_name, last_name, gender, address,
#               postcode, email, username, dob, registered_date, phone, picture))
#         logging.info(f"Data inserted for {first_name} {last_name}")

#     except Exception as e:
#         logging.error(f'could not insert data due to {e}')


# def create_spark_connection():
#     s_conn = None

#     try:
#         s_conn = SparkSession.builder \
#             .appName('SparkDataStreaming') \
#             .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
#                                            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
#             .config('spark.cassandra.connection.host', 'localhost') \
#             .getOrCreate()

#         s_conn.sparkContext.setLogLevel("ERROR")
#         logging.info("Spark connection created successfully!")
#     except Exception as e:
#         logging.error(f"Couldn't create the spark session due to exception {e}")

#     return s_conn


# def connect_to_kafka(spark_conn):
#     spark_df = None
#     try:
#         spark_df = spark_conn.readStream \
#             .format('kafka') \
#             .option('kafka.bootstrap.servers', 'localhost:9092') \
#             .option('subscribe', 'users_created') \
#             .option('startingOffsets', 'earliest') \
#             .load()
#         logging.info("kafka dataframe created successfully")
#     except Exception as e:
#         logging.warning(f"kafka dataframe could not be created because: {e}")

#     return spark_df


# def create_cassandra_connection():
#     try:
#         # connecting to the cassandra cluster
#         cluster = Cluster(['localhost'])

#         cas_session = cluster.connect()

#         return cas_session
#     except Exception as e:
#         logging.error(f"Could not create cassandra connection due to {e}")
#         return None


# def create_selection_df_from_kafka(spark_df):
#     schema = StructType([
#         StructField("id", StringType(), False),
#         StructField("first_name", StringType(), False),
#         StructField("last_name", StringType(), False),
#         StructField("gender", StringType(), False),
#         StructField("address", StringType(), False),
#         StructField("post_code", StringType(), False),
#         StructField("email", StringType(), False),
#         StructField("username", StringType(), False),
#         StructField("registered_date", StringType(), False),
#         StructField("phone", StringType(), False),
#         StructField("picture", StringType(), False)
#     ])

#     sel = spark_df.selectExpr("CAST(value AS STRING)") \
#         .select(from_json(col('value'), schema).alias('data')).select("data.*")
#     print(sel)

#     return sel


# if __name__ == "__main__":
#     # create spark connection
#     spark_conn = create_spark_connection()

#     if spark_conn is not None:
#         # connect to kafka with spark connection
#         spark_df = connect_to_kafka(spark_conn)
#         selection_df = create_selection_df_from_kafka(spark_df)
#         session = create_cassandra_connection()

#         if session is not None:
#             create_keyspace(session)
#             create_table(session)

#             logging.info("Streaming is being started...")

#             streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
#                                .option('checkpointLocation', '/tmp/checkpoint')
#                                .option('keyspace', 'spark_streams')
#                                .option('table', 'created_users')
#                                .start())

#             streaming_query.awaitTermination()
#---------------------------------------------------------------------------------------------------------------

import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.nyc_trip_data (
        trip_id TEXT PRIMARY KEY,
        pickup_datetime TEXT,
        dropoff_datetime TEXT,
        passenger_count TEXT,
        trip_distance FLOAT,
        fare_amount FLOAT);
    """)
    print("Table created successfully!")


def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'nyc_trip_data') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("trip_id", StringType(), False),
        StructField("pickup_datetime", StringType(), False),
        StructField("dropoff_datetime", StringType(), False),
        StructField("passenger_count", StringType(), True),
        StructField("trip_distance", FloatType(), True),
        StructField("fare_amount", FloatType(), True)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'nyc_trip_data')
                               .start())

            streaming_query.awaitTermination()



#-------------------------------------------------------------------------------------------------------------------------------------

# #MAIN DATA
# import logging
# import pandas as pd
# from cassandra.cluster import Cluster
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# def create_keyspace(session):
#     session.execute("""
#         CREATE KEYSPACE IF NOT EXISTS spark_streams
#         WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
#     """)
#     print("Keyspace created successfully!")


# def create_table(session):
#     session.execute("""
#     CREATE TABLE IF NOT EXISTS spark_streams.yellow_taxi_trips (
#         vendor_id INT,
#         pickup_datetime TEXT,
#         dropoff_datetime TEXT,
#         passenger_count INT,
#         trip_distance FLOAT,
#         # pickup_location TEXT,
#         # dropoff_location TEXT,
#         Rate_codeID INT,
#         store_and_fwd_flag TEXT, 
#         PU_LocationID INT, 
#         DO_LocationID INT, 
#         Payment_Type INT, 
#         fare_amount FLOAT,
#         extra INT, 
#         mta_tax FLOAT, 
#         tip_amount FLOAT,
#         tolls_amount FLOAT'
#         improvement_surcharge FLOAT,
#         total_amount FLOAT,
#         PRIMARY KEY (vendor_id, pickup_datetime)
#     );
#     """)
#     print("Table created successfully!")


# def insert_trip_data(session, **kwargs):
#     print("Inserting trip data...")

#     vendor_id = kwargs.get('vendor_id')
#     pickup_datetime = kwargs.get('pickup_datetime')
#     dropoff_datetime = kwargs.get('dropoff_datetime')
#     passenger_count = kwargs.get('passenger_count')
#     trip_distance = kwargs.get('trip_distance')
#     Rate_codeID = kwargs.get('Rate_codeID')
#     store_and_fwd_flag = kwargs.get('store_and_fwd_flag')
#     PU_LocationID = kwargs.get('PU_LocationID')
#     DO_LocationID = kwargs.get('DO_LocationID')
#     Payment_Type = kwargs.get('Payment_Type')
#     # pickup_location = kwargs.get('pickup_location')
#     # dropoff_location = kwargs.get('dropoff_location')
#     fare_amount = kwargs.get('fare_amount')
#     extra = kwargs.get('extra')
#     mta_tax = kwargs.get('mta_tax')
#     tip_amount = kwargs.get('tip_amount')
#     tolls_amount = kwargs.get('tolls_amount')
#     improvement_surcharge = kwargs.get('improvement_surcharge')
#     total_amount = kwargs.get('total_amount')

#     try:
#         session.execute("""
#             INSERT INTO spark_streams.yellow_taxi_trips (vendor_id, pickup_datetime, dropoff_datetime, passenger_count,
#                 trip_distance, Rate_codeID, store_and_fwd_flag, PU_LocationID, DO_LocationID, Payment_Type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,improvement_surcharge, total_amount)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """, (vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, Rate_codeID, store_and_fwd_flag, PU_LocationID, DO_LocationID, Payment_Type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,improvement_surcharge, total_amount))
#         logging.info(f"Data inserted for trip at {pickup_datetime}")

#     except Exception as e:
#         logging.error(f'Could not insert trip data due to {e}')


# def create_spark_connection():
#     try:
#         s_conn = SparkSession.builder \
#             .appName('SparkTaxiTripData') \
#             .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1") \
#             .config('spark.cassandra.connection.host', 'localhost') \
#             .getOrCreate()

#         s_conn.sparkContext.setLogLevel("ERROR")
#         logging.info("Spark connection created successfully!")
#         return s_conn
#     except Exception as e:
#         logging.error(f"Couldn't create the spark session due to exception {e}")
#         return None


# def read_trip_data(spark_conn, file_path):
#     # Define schema matching the Yellow Taxi data structure
#     schema = StructType([
#         StructField("VendorID", StringType(), True),
#         StructField("tpep_pickup_datetime", StringType(), True),
#         StructField("tpep_dropoff_datetime", StringType(), True),
#         StructField("passenger_count", IntegerType(), True),
#         StructField("trip_distance", FloatType(), True),
#         StructField("RatecodeID", FloatType(), True),
#         StructField("store_and_fwd_flag", FloatType(), True),
#         StructField("PULocationID", FloatType(), True),
#         StructField("DOLocationID", FloatType(), True),
#         StructField("Payment_Type", FloatType(), True),
#         # StructField("dropoff_longitude", FloatType(), True),
#         # StructField("dropoff_latitude", FloatType(), True),
#         StructField("fare_amount", FloatType(), True),
#         StructField("extra", FloatType(), True),
#         StructField("mta_tax", FloatType(), True),
#         StructField("tip_amount", FloatType(), True),
#         StructField("tolls_amount", FloatType(), True),
#         StructField("improvement_surcharge", FloatType(), True),
#         StructField("total_amount", FloatType(), True)
#     ])

#     # Read the CSV file as a Spark DataFrame
#     trip_df = spark_conn.read.csv(file_path, header=True, schema=schema)
#     trip_df = trip_df.withColumnRenamed("VendorID", "vendor_id") \
#                      .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
#                      .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
#                      .withColumnRenamed("RatecodeID", "Rate_codeID") \
#                      .withColumnRenamed("PULocationID", "PU_LocationID") \
#                      .withColumnRenamed("DOLocationID", "DO_LocationID") \
    
#     # trip_df = trip_df.withColumn("pickup_location", 
#     #                              col("pickup_latitude").cast(StringType()) + ", " + col("pickup_longitude").cast(StringType())) \
#     #                  .withColumn("dropoff_location", 
#     #                              col("dropoff_latitude").cast(StringType()) + ", " + col("dropoff_longitude").cast(StringType()))
    
#     return trip_df.select("vendor_id", "pickup_datetime", "dropoff_datetime", "passenger_count", 
#                           "trip_distance","Rate_codeID", "store_and_fwd_flag", "PU_LocationID","DO_LocationID", "Payment_Type", "fare_amount","extra", "mta_tax", "tip_amount", "tolls_amount","improvement_surcharge", "total_amount")
# def create_cassandra_connection():
#     try:
#         # Connecting to the Cassandra cluster
#         cluster = Cluster(['9042:9042'])  # Replace 'localhost' with the actual Cassandra IP if needed
#         session = cluster.connect()
#         return session
#     except Exception as e:
#         logging.error(f"Could not create Cassandra connection due to {e}")
#         return None


# if __name__ == "__main__":
#     # Create Spark and Cassandra connections
#     spark_conn = create_spark_connection()
#     session = create_cassandra_connection()

#     if spark_conn and session:
#         create_keyspace(session)
#         create_table(session)

#         # File path to the Yellow Taxi Trip data CSV file
#         file_path = "C:/Users/Jollybabe/Documents/IU/Data_Engineering_Course/Project/2018_Yellow_Taxi_Trip_Data_20240729.csv"  # Update to your actual file path

#         trip_df = read_trip_data(spark_conn, file_path)
#         trip_df.show(5)  # Display sample data for verification

#         # Iterate over the DataFrame rows and insert each record into Cassandra
#         for row in trip_df.collect():
#             insert_trip_data(session, **row.asDict())



#SMALLER DATA
import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.weather_data (
        temperature FLOAT,
        humidity INT,
        wind_speed FLOAT,
        precipitation FLOAT,
        cloud_cover TEXT,
        atmospheric_pressure FLOAT,
        uv_index INT,
        season TEXT,
        visibility FLOAT,
        location TEXT,
        weather_type TEXT,
        PRIMARY KEY (temperature, humidity)
    );
    """)
    print("Table created successfully!")

def insert_weather_data(session, **kwargs):
    try:
        session.execute("""
            INSERT INTO spark_streams.weather_data (temperature, humidity, wind_speed, precipitation,
                cloud_cover, atmospheric_pressure, uv_index, season, visibility, location, weather_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (kwargs['Temperature'], kwargs['Humidity'], kwargs['Wind Speed'], kwargs['Precipitation (%)'],
              kwargs['Cloud Cover'], kwargs['Atmospheric Pressure'], kwargs['UV Index'], kwargs['Season'],
              kwargs['Visibility (km)'], kwargs['Location'], kwargs['Weather Type']))
        logging.info(f"Data inserted for temperature {kwargs['Temperature']} and humidity {kwargs['Humidity']}")
    except Exception as e:
        logging.error(f'Could not insert weather data due to {e}')

def create_spark_connection():
    try:
        s_conn = SparkSession.builder \
            .appName('SparkWeatherData') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return s_conn
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
        return None

def read_weather_data(spark_conn, file_path):
    schema = StructType([
        StructField("Temperature", FloatType(), True),
        StructField("Humidity", IntegerType(), True),
        StructField("Wind Speed", FloatType(), True),
        StructField("Precipitation (%)", FloatType(), True),
        StructField("Cloud Cover", StringType(), True),
        StructField("Atmospheric Pressure", FloatType(), True),
        StructField("UV Index", IntegerType(), True),
        StructField("Season", StringType(), True),
        StructField("Visibility (km)", FloatType(), True),
        StructField("Location", StringType(), True),
        StructField("Weather Type", StringType(), True)
    ])

    df = spark_conn.read.csv(file_path, header=True, schema=schema)
    return df.select("Temperature", "Humidity", "Wind Speed", "Precipitation (%)", "Cloud Cover",
                     "Atmospheric Pressure", "UV Index", "Season", "Visibility (km)", "Location", "Weather Type")

def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])  # Update to the actual Cassandra IP if needed
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None

if __name__ == "__main__":
    spark_conn = create_spark_connection()
    session = create_cassandra_connection()

    if spark_conn and session:
        create_keyspace(session)
        create_table(session)

        file_path = 'C:/Users/Jollybabe/Documents/IU/Data_Engineering_Course/Project/weather_classification_data.csv'

        weather_df = read_weather_data(spark_conn, file_path)
        weather_df.show(5)

        for row in weather_df.collect():
            insert_weather_data(session, **row.asDict())
