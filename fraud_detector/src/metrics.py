from prometheus_client import Counter, Gauge, Histogram


TRANSACTIONS = Counter('scoring_transactions_total', 'Transactions scored and sent to Kafka')
FRAUD = Counter('scoring_fraud_total', 'Transactions flagged as fraud')
DURATION = Histogram('scoring_transaction_duration_seconds', 'Processing time per transaction')
SCORES = Histogram('scoring_score', 'Fraud scores', buckets=(0.01, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1))
PUBLISH_DURATION = Histogram('scoring_kafka_publish_duration_seconds', 'Time to publish a score to Kafka')
MODEL_RELOADS = Counter('scoring_model_reloads_total', 'Model replacements', ['version'])
MODEL_RELOAD_TIME = Gauge('scoring_model_reload_timestamp_seconds', 'Time of model replacement', ['version'])
