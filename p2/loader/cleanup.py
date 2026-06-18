from pathlib import Path

RAW_DIR = Path("/opt/airflow/raw")

def borrar_parquets_raw() -> None:
    # elimina todos los parquet generados en raw que tengan mas de una hora existiendo
    # se mide el "tiempo de existencia" con la fecha de modificacion
    limite = datetime.now() - timedelta(hours=1)

    for archivo in RAW_DIR.glob("*"):
        if datetime.fromtimestamp(archivo.stat().st_mtime) < limite:
            archivo.unlink()
