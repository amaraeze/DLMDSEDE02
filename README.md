# DLMDSEDE02

Realtime Data Streaming | End-to-End Data Engineering Project

This project centres around implementing  robust, end-to-end data engineering pipeline. It covers every stage of the pipeline, from data ingestion to processing and storage, with a tech stack that includes Docker, Apache Airflow, Kafka, Zookeeper, Spark, and Cassandra for efficient deployment and scalability.

The pipeline begins by pulling data from https://data.cityofnewyork.us/resource/pkmi-4kfn.json API.
I have selected the Taxi Trip Data provided by the NYC Taxi and Limousine Commission. This dataset includes comprehensive records of taxi trips within New York City, capturing critical details such as timestamps, pickup and drop-off locations, fare amounts, trip distances, and passenger counts.

I chose this dataset because it presents a real-world example of high-volume, time-stamped data that is ideal for demonstrating the capabilities of a robust, real-time data processing architecture.


I have chosen Apache Kafka for data ingestion. Kafka acts as the streaming platform, transmitting data from PostgreSQL to the data processing engine. Zookeeper coordinates the Kafka brokers, ensuring reliable data streaming and scalability.

I utilized PostgreSQL for structured data and Cassandra for high throughput storage, both running in Docker containers. Data processing and aggregation will be managed by Apache Spark, also containerized, which will leverage its streaming capabilities to perform real-time transformations and windowing functions. Spark is employed for data processing tasks, with both master and worker nodes configured to perform parallel data processing. This setup ensures efficient handling of large data volumes.

To ensure the system’s reliability, scalability, and maintainability, I implemented horizontal scaling for Spark and clustered setups for Kafka and Cassandra. Docker orchestration with Kubernetes will facilitate seamless scaling and management of containers. For data security, governance, and protection, I used encrypted data streams, role-based access controls, and compliance monitoring tools.

Apache Airflow is responsible for orchestrating the data pipeline. It schedules and manages the data ingestion workflow, storing fetched data in a PostgreSQL database for further processing.



Control Center enables monitoring of Kafka streams, while the Schema Registry manages the data schemas to ensure consistency in data format.



It is fully containerized using Docker to simplify deployment, making the entire pipeline easily scalable and manageable. Each component runs as a separate Docker container, enabling smooth interactions and flexibility in deployment.

To view the source code, visit GitHub repository: https://github.com/amaraeze/DLMDSEDE02.git
