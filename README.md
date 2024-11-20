# DLMDSEDE02

Realtime Data Streaming | End-to-End Data Engineering Project

![data_flowchart_docker](https://github.com/user-attachments/assets/8d02063d-50d7-42bf-8e54-127349f7ecf9)

This project centers on implementing a robust, end-to-end data engineering pipeline, covering every stage from data ingestion to processing and storage. The tech stack includes Docker, Apache Airflow, Kafka, Zookeeper, Spark, and Cassandra, all of which contribute to efficient deployment and scalability.

The pipeline starts by pulling data from the NYC Taxi Trip Data API, provided by the NYC Taxi and Limousine Commission. This dataset contains detailed records of taxi trips within New York City, capturing critical information such as timestamps, pickup and drop-off locations, fare amounts, trip distances, and passenger counts.

I selected this dataset because it provides a real-world example of high-volume, time-stamped data, making it ideal for demonstrating the capabilities of a real-time, scalable data processing architecture.

For data ingestion, I’ve chosen Apache Kafka as the streaming platform, which transmits data from PostgreSQL to the processing engine. Zookeeper coordinates the Kafka brokers, ensuring reliable data streaming and scalability.

PostgreSQL is used for structured data storage, while Cassandra is employed for high-throughput storage, both running in Docker containers. Data processing and aggregation are handled by Apache Spark, also containerized, which leverages its streaming capabilities for real-time transformations and windowing functions. With both master and worker nodes configured, Spark ensures efficient parallel data processing, allowing for seamless handling of large data volumes.

To ensure system reliability, scalability, and maintainability, I’ve implemented horizontal scaling for Spark and clustered setups for Kafka and Cassandra. Docker orchestration with Kubernetes simplifies the scaling and management of containers. For data security, governance, and protection, I’ve implemented encrypted data streams, role-based access controls, and compliance monitoring tools.

Apache Airflow orchestrates the entire pipeline, managing the scheduling and execution of data ingestion workflows and storing the fetched data in PostgreSQL for further processing.

The Control Center enables monitoring of Kafka streams, while the Schema Registry ensures data consistency by managing the schemas for the streaming data.

The entire pipeline is fully containerized with Docker, simplifying deployment and enabling scalability and flexibility. Each component operates as a separate Docker container, facilitating smooth interactions and easy management of the overall system


To view the source code, visit GitHub repository: https://github.com/amaraeze/DLMDSEDE02.git
