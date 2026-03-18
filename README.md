
Automated CBN Macro-Economic Data Pipeline

Tech Stack: Python, Apache Airflow, Spark (PySpark), Docker, HDFS, PostgreSQL.


This is an end-to-end data pipeline that ingests Nigerian economic data using the Central Bank of Nigeria (CBN)'s API endpoint  through a distributed Spark cluster.
The project uses the medallion architecture with the bronze and silver layers in hdfs (hadoop) and then the transformed data is finally into a gold layer in a  PostgreSQL database for further analytics.



2. Architecture
Orchestration: Airflow  running in Docker.

Ingestion: Python Requests pulling data from API endpoint.

Storage: Hadoop HDFS.

Processing: PySpark (Transformations).

Serving: PostgreSQL Database.



3. Major Challenges & Solutions

Challenge: Docker-in-Docker pathing and permission "Access Denied" errors during volume mounting.

Solution: Implemented internal container bridging (/opt/spark-apps) and configured the Docker socket (666) to allow the Airflow Scheduler to spawn Spark containers dynamically.

Challenge: SSL/Handshake failures during driver downloads in docker.

Solution: had to download the .jar file (postgresql-42.7.2.jar) which was placed in the spark-apps folder and injected via --jars.

