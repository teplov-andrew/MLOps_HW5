# MLOps HW5

## Запуск

```bash
docker compose up -d --build
docker compose ps -a
```
Система начнёт отправлять одну транзакцию в секунду.

Откройте [Grafana](http://localhost:3000), логин и пароль — `admin`.
В папке **HW5** есть три дашборда.

- **Transaction Processing** - TPS, перцентиль 95 времени обработки, доля фрода, тепловая карта скоров и смена версии модели. Фильтры **State** и **Merchant** управляют панелями TPS и плотности скоров из PostgreSQL. Отдельная панель показывает долю фрода по категориям среди последних 1000 результатов.
- **Score Storage** - скорость записи, пернцентиль 95 времени записи, число флагов фрода, тепловая карта записанных скоров. Таблица показывает число скоров и долю фрода по версиям модели за выбранный период.
- **System Metrics** - загрузка CPU, память, трафик сети контейнера Node Exporter и заполненность диска.

## Метрики

В [Prometheus](http://localhost:9090/targets) дождитесь статуса UP у трёх целей. Интервал сбора — 1 секунда. Откройте [метрики скоринга](http://localhost:8005/metrics) и [метрики записи](http://localhost:8006/metrics). При обновлении страниц растут `scoring_transactions_total` и `writer_scores_total`.

- Скоринг — `scoring_transactions_total`, `scoring_fraud_total`, `scoring_transaction_duration_seconds`, `scoring_score`
- Запись — `writer_scores_total`, `writer_fraud_total`, `writer_write_duration_seconds`, `writer_score`
- Смена модели — `scoring_model_reloads_total{version}` и `scoring_model_reload_timestamp_seconds{version}`
- Node Exporter — `node_cpu_seconds_total`, `node_memory_MemAvailable_bytes`, `node_memory_MemTotal_bytes`, `node_network_receive_bytes_total`, `node_network_transmit_bytes_total`, `node_filesystem_avail_bytes`, `node_filesystem_size_bytes`

## Смена модели

Откройте [Airflow](http://localhost:8085), логин и пароль — `admin`
Запустите `add_label_batch` и дождитесь его завершения. Затем запустите `train_model`
Airflow записывает сравнение метрик качества и публикует обученную модель,
чтобы смену версии можно было воспроизвести. Публикация создаёт новый файл
модели и обновляет `current.json`

```bash
docker compose exec fraud_detector cat /opt/project/shared/models/current.json
docker compose logs --tail=20 fraud_detector
```

На следующей транзакции скоринг подхватит модель. В таблице PostgreSQL появится строка для новой версии

## Остановка

```bash
docker compose down
```
