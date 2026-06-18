from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from loader.stream_manager import loader, extractor, transformer

def run_pipeline():
    for table, contenido in loader():
        df = extractor(table, contenido)
        transformer(table, df)

with DAG(
    dag_id="descargar",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gdelt"],
) as dag:
  descargar = PythonOperator(
        task_id="descargar",
        python_callable=descargar_datos
    )

    borrar = PythonOperator(
        task_id="borrar",
        python_callable=eliminar_datos
    )
    descargar >> borrar
