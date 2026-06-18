from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from loader.stream_manager import main
from loader.cleanup import borrar_parquets_raw

with DAG(
    dag_id="descargar",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gdelt"],
) as dag:
  descargar = PythonOperator(
        task_id="descargar",
        python_callable=main
    )

    borrar = PythonOperator(
        task_id="borrar",
        python_callable=borrar_parquets_raw
    )
    descargar >> borrar
