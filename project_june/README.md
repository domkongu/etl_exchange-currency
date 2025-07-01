# Project Structure

```
project_june/
│
├── dags/
│   └── etl_pipeline.py
├── scripts/
│   ├── fetch_exchange_rates.py
│   └── load_to_postgres.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Quick Start

1. Clone the repository
2. Run `docker-compose up` to start Airflow and Postgres.
3. Access Airflow UI at [http://localhost:8080](http://localhost:8080) (login: admin/admin).
4. The ETL pipeline will fetch exchange rates and load them into Postgres.

## Configuration
- Update the API key in `fetch_exchange_rates.py` if needed.


--- # etl_exchange-currency
