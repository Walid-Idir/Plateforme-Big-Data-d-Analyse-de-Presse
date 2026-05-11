"""
Airflow DAG — Streaming Pipeline (Kafka)

Manually triggered (schedule_interval=None).
Tasks:
  1. produce_to_kafka   — Read Bronze files, publish to Kafka topic 'news'
  2. consume_and_enrich — Consume from Kafka, enrich with NLP, write Silver+Gold
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 0,
}

BASE = "/app"

with DAG(
    dag_id="news_streaming_pipeline",
    default_args=default_args,
    description="Streaming pipeline: Kafka producer → consumer with NLP enrichment",
    schedule_interval=None,   # triggered manually or by event
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["news", "streaming", "kafka"],
) as dag:

    produce = BashOperator(
        task_id="produce_to_kafka",
        bash_command=f"python {BASE}/src/streaming/kafka_producer.py",
        env={
            "KAFKA_BROKER": "kafka:9092",
            "KAFKA_TOPIC": "news",
        },
    )

    consume = BashOperator(
        task_id="consume_and_enrich",
        bash_command=f"python {BASE}/src/streaming/kafka_consumer.py",
        env={
            "KAFKA_BROKER": "kafka:9092",
            "KAFKA_TOPIC": "news",
        },
    )

    produce >> consume
