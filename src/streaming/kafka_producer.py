"""Kafka producer: reads bronze JSON files and publishes them to topic 'news'."""
import json
import time
from pathlib import Path
from kafka import KafkaProducer
import os

def send_file(producer, topic, path: Path):
    with open(path, 'r', encoding='utf-8') as f:
        obj = json.load(f)
    producer.send(topic, value=obj)

def main():
    broker = os.getenv('KAFKA_BROKER', 'localhost:9092')
    topic = os.getenv('KAFKA_TOPIC', 'news')
    prod = KafkaProducer(bootstrap_servers=[broker], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    bronze = Path('data/bronze')
    for f in sorted(bronze.glob('*.json')):
        try:
            send_file(prod, topic, f)
            print('Sent', f)
            time.sleep(0.1)
        except Exception as e:
            print('Err sending', f, e)
    prod.flush()

if __name__ == '__main__':
    main()
