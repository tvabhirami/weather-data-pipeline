# DAG: weather_data_pipeline.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Run weather data scripts in sequence and parallel using Airflow',
    schedule_interval='@hourly',
    catchup=False
)

run_producer = BashOperator(
    task_id='run_producer_script',
    bash_command='/home/hp/WEATHER/airflow_venv_310/bin/python /home/hp/WEATHER/producer1.py '
                 '>> /home/hp/WEATHER/producer_log.txt 2>&1',
    dag=dag
)

run_spark_stream = BashOperator(
    task_id='run_spark_consumer',
    bash_command="""
    /home/hp/spark/spark-3.5.4-bin-hadoop3/bin/spark-submit \
    --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.11.1026 \
    /home/hp/WEATHER/spark_consumer.py >> /home/hp/WEATHER/spark_log.txt 2>&1
""",

    dag=dag
)

run_kafka_consumer = BashOperator(
    task_id='run_consumer_script',
    bash_command='/home/hp/WEATHER/airflow_venv_310/bin/python /home/hp/WEATHER/consumer1.py '
                 '>> /home/hp/WEATHER/consumer_log.txt 2>&1',
    dag=dag
)

run_producer >> [run_spark_stream, run_kafka_consumer]