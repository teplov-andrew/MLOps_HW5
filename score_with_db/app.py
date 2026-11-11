import json
import logging
import os

import psycopg
from confluent_kafka import Consumer
from prometheus_client import Counter, Histogram, start_http_server


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
SAVED = Counter('writer_scores_total', 'Scores written to PostgreSQL')
FRAUD = Counter('writer_fraud_total', 'Written scores flagged as fraud')
DURATION = Histogram('writer_write_duration_seconds', 'Time to write and commit one score')
SCORES = Histogram('writer_score', 'Written scores', buckets=(0.01, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1))


def main():
    with psycopg.connect(
        host='postgres', dbname='fraud', user='fraud',
        password=os.environ['POSTGRES_PASSWORD'], connect_timeout=10,
    ) as connection:
        consumer = Consumer({
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'score-with-db',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })
        consumer.subscribe(['scores'])
        start_http_server(8000)
        logging.info('Waiting for scores')
        try:
            while True:
                message = consumer.poll(1)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(message.error())
                result = json.loads(message.value())
                fields = {name: value.decode() for name, value in message.headers() or []}
                with DURATION.time():
                    connection.execute("""
                        INSERT INTO transaction_scores
                            (transaction_id, score, fraud_flag, model_version, us_state, merch, cat_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (transaction_id) DO UPDATE SET
                            score = EXCLUDED.score,
                            fraud_flag = EXCLUDED.fraud_flag,
                            model_version = EXCLUDED.model_version,
                            us_state = EXCLUDED.us_state,
                            merch = EXCLUDED.merch,
                            cat_id = EXCLUDED.cat_id
                    """, (
                        result['transaction_id'], result['score'], result['fraud_flag'],
                        result['model_version'], fields.get('us_state', 'unknown'),
                        fields.get('merch', 'unknown'), fields.get('cat_id', 'unknown'),
                    ))
                    connection.commit()
                SAVED.inc()
                FRAUD.inc(result['fraud_flag'])
                SCORES.observe(result['score'])
                consumer.commit(message=message, asynchronous=False)
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()


if __name__ == '__main__':
    main()
