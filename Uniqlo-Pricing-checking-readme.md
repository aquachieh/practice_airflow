# Uniqlo Pricing checking

## service-up

```bash
docker-compose -f docker-compose-local-executor.yaml up

# first time only: create user: uniqlo
docker compose -f docker-compose-local-executor.yaml exec airflow bash
airflow users  create --role Admin --username uniqlo --email admin --firstname admin --lastname admin --password uniqlo
```

for first time start, enable an trigger create-uniqlo-price-tracking-tables

enable dags

- fetch-latest-product-info
- update-tracking-product-price

## three dags were implemnted in airflow

dags/folders

- create tables (needed only first time)
- fetch latest products (fetch latest & hot products from tw)
- update tracking product prices (for product were labeled as is_tracking we trakcing it every day)

## streamlit application

```bash
cd sqlite;
streamlit run products.py --server.port 5566
```
