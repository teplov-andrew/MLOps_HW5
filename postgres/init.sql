CREATE USER fraud WITH PASSWORD 'fraud';
CREATE USER grafana WITH PASSWORD 'grafana';
CREATE DATABASE fraud OWNER fraud;

\connect fraud

SET ROLE fraud;
CREATE TABLE transaction_scores (
    transaction_id TEXT PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL,
    fraud_flag INTEGER NOT NULL,
    model_version TEXT NOT NULL,
    us_state TEXT NOT NULL,
    merch TEXT NOT NULL,
    cat_id TEXT NOT NULL,
    scored_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON transaction_scores (scored_at);
GRANT SELECT ON transaction_scores TO grafana;
