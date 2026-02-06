from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

# Basic info about the DAG
default_args = {
    'owner': 'femi',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cbn_economic_pipeline',
    default_args=default_args,
    schedule_interval='@monthly', # Run once a month
    catchup=False
)


local_spark_apps_path = '/mnt/c/Users/USER/Documents/Data Engineering/cbn/spark-apps' 

# Define the task to submit the Spark job using DockerOperator
submit_spark_job = DockerOperator(
    task_id='submit_spark_job',
    image='quay.io/jupyter/pyspark-notebook:latest', # Use the image that has Spark
    api_version='auto',
    auto_remove=True,
    # The command to run your ETL script
    command="bash -c 'pip install hdfs requests && spark-submit --packages org.postgresql:postgresql:42.7.2 /opt/spark-apps/master_etl.py'",
    docker_url='unix://var/run/docker.sock',
    network_mode='cbn_hadoopnet', # MUST be on the same network as HDFS/Postgres
    mounts=[
        # Mount the script folder so the container can see the code
        Mount(source=local_spark_apps_path, target='/opt/spark-apps', type='bind'),
    ],
    dag=dag
)