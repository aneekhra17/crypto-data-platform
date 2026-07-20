"""
Schedules the crypto producer to run once every hour, instead of
requiring a terminal window left open indefinitely.

Two tasks: the producer fetches CoinGecko data and publishes to Kafka
(or Kinesis Firehose as fallback); the consumer then drains whatever
landed in the Kafka topic into S3 -- publishing to Kafka alone doesn't
move data into S3 on its own, something needs to read and land it.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_ingestion",
    default_args=default_args,
    description="Poll CoinGecko, publish to Kafka/Kinesis, drain into S3, every hour",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["crypto", "ingestion"],
) as dag:

    run_producer_once = BashOperator(
        task_id="run_crypto_producer_once",
        bash_command=(
            "pip install --quiet requests kafka-python-ng boto3 && "
            "python /opt/airflow/ingestion/producer/crypto_producer.py "
            "--kafka-bootstrap kafka:9093 "
            "--run-once"
        ),
    )

    drain_kafka_to_datalake = BashOperator(
        task_id="drain_kafka_to_datalake",
        bash_command=(
            "pip install --quiet kafka-python-ng boto3 && "
            "python /opt/airflow/ingestion/consumer/crypto_consumer.py "
            "--bootstrap-servers kafka:9093 "
            "--bucket crypto-platform-datalake "
            "--run-once"
        ),
    )

    run_producer_once >> drain_kafka_to_datalake
