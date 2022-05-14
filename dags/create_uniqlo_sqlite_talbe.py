from datetime import datetime

from airflow import DAG
# from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

with DAG(
    dag_id='create-uniqlo-price-tracking-tables',
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    tags=['uniqlo'],
    catchup=False,
) as dag:
    # Example of creating a task that calls a common CREATE TABLE sql command.
    create_uniqlo_product_table = SqliteOperator(
        task_id='create_uniqlo_product_sqlite',
        sql=r"""
        CREATE TABLE UniqloProducts(
            product_id_tw TEXT PRIMARY KEY,
            l1_id TEXT,
            title TEXT,
            product_image_url TEXT,
            is_tracking BOOL,
            meta TEXT,
            appear_date TEXT
        );
        """,
        dag=dag,
    )

    # Example of creating a task that calls a common CREATE TABLE sql command.
    create_product_price_table = SqliteOperator(
        task_id='create_uniqlo_product_price_sqlite',
        sql=r"""
        CREATE TABLE UniqloProductPrice (
            product_id_tw TEXT,
            date TEXT,
            price REAL,
            region TEXT,
            currency TEXT,
            FOREIGN KEY(product_id_tw) REFERENCES UniqloProducts(product_id_tw)
        );
        """,
        dag=dag,
    )

    # Example of creating a task that calls a common CREATE TABLE sql command.
    create_product_rank_table = SqliteOperator(
        task_id='create_uniqlo_product_rank_sqlite',
        sql=r"""
        CREATE TABLE ProductRanks (
            product_id_tw TEXT,
            date TEXT,
            rank INTEGER,
            region TEXT,
            FOREIGN KEY(product_id_tw) REFERENCES UniqloProducts(product_id_tw)
        );
        """,
        dag=dag,
    )

    create_uniqlo_product_table >> create_product_price_table
    create_uniqlo_product_table >> create_product_rank_table