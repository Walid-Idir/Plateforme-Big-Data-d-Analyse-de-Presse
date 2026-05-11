"""Kafka consumer: consumes news messages, enriches with NLP and writes to silver/gold directories."""
import json
import os
from pathlib import Path
from kafka import KafkaConsumer

from src.enrich import nlp


def main():
    broker = os.getenv('KAFKA_BROKER', 'localhost:9092')
    topic = os.getenv('KAFKA_TOPIC', 'news')
    consumer = KafkaConsumer(topic, bootstrap_servers=[broker], value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    silver = Path('data/silver')
    gold = Path('data/gold')
    silver.mkdir(parents=True, exist_ok=True)
    gold.mkdir(parents=True, exist_ok=True)
    for msg in consumer:
        obj = msg.value
        # enrichment
        obj['keywords'] = nlp.extract_keywords(obj.get('content') or obj.get('content_clean',''))
        obj['entities'] = nlp.extract_entities(obj.get('content') or obj.get('content_clean',''))
        # save to silver
        fname = obj.get('url','').split('/')[-1] or str(hash(obj.get('title','')))
        sfile = silver / (fname + '.json')
        with open(sfile, 'w', encoding='utf-8') as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        # example: also write to gold (aggregation placeholder)
        gfile = gold / (fname + '.json')
        with open(gfile, 'w', encoding='utf-8') as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        print('Processed message ->', sfile)


if __name__ == '__main__':
    main()
