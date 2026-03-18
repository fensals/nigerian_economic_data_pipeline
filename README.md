*Nigerian Economic Indicators - Data Pipeline*

*Project Overview*

This project is an end-to-end data engineering pipeline that automates the collection, transformation, and storage of Nigerian economic indicators (Exchange Rates and Inflation). Using a Medallion Architecture, the pipeline converts raw API data into business-ready insights, providing a "Gold" standard dataset for economic analysis and growth tracking.

*Tech Stack*

Orchestration: Apache Airflow

Processing: Apache Spark (PySpark)

Storage: Hadoop HDFS (Data Lake)

Database: PostgreSQL (Data Warehouse)

Containerization: Docker

Environment: WSL2 / Ubuntu


*Data Architecture*

The pipeline follows the Medallion (Multi-Hop) Architecture to ensure data quality and lineage:


Bronze (Landing):

Python scripts fetch raw JSON data from the Central Bank of Nigeria (CBN) API.

Data is stored in HDFS at /cbn_project/landing/ in its original format to allow for future reprocessing.


Silver (Cleaned):

Spark processes the raw JSON, enforcing schemas and handling data types (Dates, Decimals).

Filtering logic extracts specific indicators (e.g., US Dollar rates).

Stored as Parquet files in HDFS for high-performance distributed reads.


Gold (Analytics):

Business logic is applied: calculating Volatility and Growth Indices (Base-100).

Data is joined into a unified macro-economic table.

The final dataset is loaded into PostgreSQL for BI and visualization.

Orchestration
The pipeline is managed by an Airflow DAG (cbn_economic_pipeline) with the following features:

Containerized Execution: Airflow triggers a temporary Docker container to run Spark jobs, ensuring a clean production environment.

Idempotency: The pipeline is designed to be "Reset-Ready." It can be cleared and re-run for any period without duplicating data or causing errors.

Error Handling: Implements HDFS permission management and path verification to ensure robust execution.

Key Features
Automated Scaling: Uses Spark for distributed computing, capable of handling millions of economic records.

Data Lineage: Tracks when data was processed using metadata columns.

Pro-active Schema Enforcement: Prevents "dirty data" from reaching the final analytics table.

Sample Analytics
The "Gold" table provides metrics such as:

USD Growth Index: Tracking the cumulative devaluation of the Naira against the USD.

Inflation vs. FX Correlation: Analyzing how food inflation responds to exchange rate volatility.

How to Run
Clone the Repo: git clone 

Start the Stack: docker-compose up -d

Trigger the DAG: Access Airflow at localhost:8082 and turn on cbn_economic_pipeline.
