import logging
import os

import joblib

from fraud_detector.src.metrics import MODEL_RELOADS, MODEL_RELOAD_TIME
from pipeline.storage import ROOT, current_model


model = None
model_version = None
threshold = float(os.getenv('FRAUD_THRESHOLD', '0.5'))


def make_pred(df):
    global model, model_version
    current = current_model()
    if current['version'] != model_version:
        previous_version = model_version
        model = joblib.load(ROOT / 'models' / current['file'])
        model_version = current['version']
        if previous_version is not None:
            MODEL_RELOADS.labels(version=model_version).inc()
            MODEL_RELOAD_TIME.labels(version=model_version).set_to_current_time()
        logging.info('Loaded model %s', model_version)
    score = float(model.predict_proba(df)[0, 1])
    res = {'score': score, 'fraud_flag': int(score >= threshold), 'model_version': model_version}
    return res
