from pathlib import Path
import requests
import pandas as pd
import io

URL        = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
OUTPUT_DIR = Path("/opt/airflow/raw")
TABLES     = ["events", "gkg", "mentions"]

def loader() -> list[tuple[str, bytes]]:
    # descarga el indice y retorna (table, contenido_zip) por cada tabla
    texto_indice = requests.get(URL)
    texto_indice.raise_for_status()
    resultados = []
    for linea in texto_indice.text.splitlines():
        peso, codigo, enlace = linea.split()
        enlace_lower = enlace.lower()
        table = "events" if ".export." in enlace_lower else next((t for t in TABLES if t in enlace_lower), None)
        if not table:
            continue
        archivo = requests.get(enlace)
        archivo.raise_for_status()
        resultados.append((table, archivo.content))
    return resultados

def extractor(table: str, contenido: bytes) -> pd.DataFrame:
    # zip -> csv en memoria
    df = pd.read_csv(io.BytesIO(contenido), compression="zip", sep="\t", low_memory=False, header=None)
    df.columns = [str(c).lower() for c in df.columns]
    return df

def transformer(table: str, df: pd.DataFrame) -> None:
    # csv -> parquet en disco
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_DIR / f"{table}.parquet", index=False, engine="pyarrow")
    print(f"{table} -> ok")

def main():
    for table, contenido in loader():
        df = extractor(table, contenido)
        transformer(table, df)

if __name__ == "__main__":
    main()
