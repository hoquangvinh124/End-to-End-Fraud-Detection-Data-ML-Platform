# MLOps Fraud Detection Platform

## Local stack

Copy `.env.example` to `.env`, set the optional Discord webhook, then run:

```powershell
docker compose up -d --build
```

Operational UIs: Grafana `http://localhost:3000`, Prometheus `http://localhost:9090`,
Alertmanager `http://localhost:9093`, Airflow `http://localhost:8092`, and Trino
`http://localhost:8090`. CDC ingestion runs continuously as the
`cdc-transactions` and `cdc-fraud-cases` services; Airflow schedules only the
bounded Bronze→Silver→Gold→Redis batch pipeline.
