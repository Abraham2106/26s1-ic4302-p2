from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from loader.stream_manager import main
from loader.cleanup import borrar_parquets_raw

with DAG(
    dag_id="gdelt_pipeline_15min",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gdelt", "pipeline"],
) as dag:

    descargar = PythonOperator(
        task_id="descargar_gdelt",
        python_callable=main
    )

    limpiar_raw = PythonOperator(
        task_id="limpiar_raw_viejo",
        python_callable=borrar_parquets_raw
    )

    descargar >> limpiar_raw
