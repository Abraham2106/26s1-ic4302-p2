import pandas as pd
import pyarrow.parquet as pq
import json
from pathlib import Path

JSON_DIR   = Path("26s1-ic4302-p2/p2/raw")
OUTPUT_DIR = Path("26s1-ic4302-p2/p2/data")
TABLES     = ["events", "gkg", "mentions"]

def json_to_parquet(json_path: Path, output_dir: Path) -> tuple[Path, pd.DataFrame]:
    data = json.loads(json_path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = [data]
    df = pd.DataFrame(data)
    df.columns = [c.lower() for c in df.columns]
    parquet_path = output_dir / f"{json_path.stem}.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    return parquet_path, df

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for table in TABLES:
        json_path = JSON_DIR / f"{table}.json"
        if not json_path.exists():
            print(f"La {table} no fue encontrada")
            continue
        try:
            parquet_path, df = json_to_parquet(json_path, OUTPUT_DIR)
            print(f"Se logro convertir el json de {table} en el dir: {OUTPUT_DIR}")
        except Exception as e:
            print(f"[{table}] ERROR: {e}")

if __name__ == "__main__":
    main()