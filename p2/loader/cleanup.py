from pathlib import Path

RAW_DIR = Path("/opt/airflow/raw")

def borrar_parquets_raw() -> None:
    # elimina todos los parquet generados en raw
    if not RAW_DIR.exists():
        return
    for parquet in RAW_DIR.glob("*.parquet"):
        parquet.unlink()
