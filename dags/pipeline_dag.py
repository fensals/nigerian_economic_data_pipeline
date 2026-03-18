import os
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

#path to location on local PC
local_spark_apps_path = 'C:/Users/USER/Documents/Data Engineering/cbn/spark-apps'


#spark job using docker operator to run the master_etl.py script inside a Docker container

submit_spark_job = DockerOperator(
    task_id='submit_spark_job',
    image='quay.io/jupyter/pyspark-notebook:latest', # Use the image that has Spark
    api_version='auto',
    auto_remove=True,
    mount_tmp_dir=False,  # <--- FIXES the "Bad Request" /tmp error
    command="bash -c 'pip install hdfs requests && spark-submit --jars /opt/spark-apps/postgresql-42.7.2.jar /opt/spark-apps/master_etl.py'",
    docker_url='unix://var/run/docker.sock',

    network_mode='cbn_hadoopnet',
    dns=['8.8.8.8'],
    environment={
        'DB_USER': os.getenv('DB_USER'),
        'DB_PASSWORD': os.getenv('DB_PASSWORD'),
    },
    
    mounts=[
        Mount(source=local_spark_apps_path, target='/opt/spark-apps', type='bind'),
    ],
    dag=dag
)