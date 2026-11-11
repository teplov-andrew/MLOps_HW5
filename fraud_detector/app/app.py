import json
import logging
import os
from time import perf_counter

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import start_http_server

from fraud_detector.src.metrics import DURATION, FRAUD, PUBLISH_DURATION, SCORES, TRANSACTIONS
from fraud_detector.src.scorer import make_pred


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
BROKERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
INPUT_TOPIC = 'transactions'
OUTPUT_TOPIC = 'scores'


def main():
    consumer = KafkaConsumer(
        INPUT_TOPIC, bootstrap_servers=BROKERS, group_id='fraud-scorer',
        auto_offset_reset='earliest', enable_auto_commit=False,
        max_poll_records=1, value_deserializer=lambda value: json.loads(value),
    )
    producer = KafkaProducer(
        bootstrap_servers=BROKERS, acks='all',
        value_serializer=lambda value: json.dumps(value).encode('utf-8'),
    )
    start_http_server(8000)
    logging.info('Waiting for transactions')
    try:
        for message in consumer:
            started = perf_counter()
            transaction = message.value
            data = transaction['data']
            result = make_pred(pd.DataFrame([data]))
            result['transaction_id'] = transaction['transaction_id']
            headers = [
                (name, str(data.get(name, 'unknown')).encode())
                for name in ('us_state', 'merch', 'cat_id')
            ]
            with PUBLISH_DURATION.time():
                producer.send(OUTPUT_TOPIC, key=result['transaction_id'].encode(),
                              value=result, headers=headers).get(timeout=30)
            consumer.commit()
            TRANSACTIONS.inc()
            FRAUD.inc(result['fraud_flag'])
            SCORES.observe(result['score'])
            DURATION.observe(perf_counter() - started)
            logging.info('Scored transaction: score=%.6f, fraud_flag=%s, model_version=%s',
                         result['score'], result['fraud_flag'], result['model_version'])
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.close()


if __name__ == '__main__':
    main()
