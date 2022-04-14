from datetime import datetime

from airflow import DAG
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python import PythonOperator

from functions import save_transactions_from_api, compute_total_amount_eur_to_btc


with DAG(
    "ingest_transactions",
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
) as dag:
    save_transactions = PythonOperator(
        task_id="save_transactions",
        python_callable=save_transactions_from_api,
    )

    latest_only = LatestOnlyOperator(task_id="latest_only")

    compute_total_amount = PythonOperator(
        task_id="compute_total_amount",
        python_callable=compute_total_amount_eur_to_btc,
    )

    save_transactions >> compute_total_amount
    latest_only >> compute_total_amount
