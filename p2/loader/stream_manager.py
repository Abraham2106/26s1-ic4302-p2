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
        table = "events"+codigo if ".export." in enlace_lower else next((t for t in TABLES if t in enlace_lower), None)
        if not table:
            continue
        archivo = requests.get(enlace)
        archivo.raise_for_status()
        resultados.append((table, archivo.content))
    return resultados

def extractor(table: str, contenido: bytes) -> pd.DataFrame:
    df = pd.read_csv(
        io.BytesIO(contenido),
        compression="zip",
        sep="\t",
        low_memory=False,
        header=None
    )

    COLUMNAS = {
        "events": [
            "globaleventid", "sqldate", "monthyear", "year", "fractiondate",
            "actor1code", "actor1name", "actor1countrycode", "actor1knowngroupcode",
            "actor1ethniccode", "actor1religion1code", "actor1religion2code",
            "actor1type1code", "actor1type2code", "actor1type3code",
            "actor2code", "actor2name", "actor2countrycode", "actor2knowngroupcode",
            "actor2ethniccode", "actor2religion1code", "actor2religion2code",
            "actor2type1code", "actor2type2code", "actor2type3code",
            "isrootevent", "eventcode", "eventbasecode", "eventrootcode",
            "quadclass", "goldsteinscale", "nummentions", "numsources",
            "numarticles", "avgtone",
            "actor1geo_type", "actor1geo_fullname", "actor1geo_countrycode",
            "actor1geo_adm1code", "actor1geo_adm2code", "actor1geo_lat",
            "actor1geo_long", "actor1geo_featureid",
            "actor2geo_type", "actor2geo_fullname", "actor2geo_countrycode",
            "actor2geo_adm1code", "actor2geo_adm2code", "actor2geo_lat",
            "actor2geo_long", "actor2geo_featureid",
            "actiongeo_type", "actiongeo_fullname", "actiongeo_countrycode",
            "actiongeo_adm1code", "actiongeo_adm2code", "actiongeo_lat",
            "actiongeo_long", "actiongeo_featureid",
            "dateadded", "sourceurl"
        ],
        "mentions": [
            "globaleventid", "eventtimedate", "mentiontimedate", "mentiontype",
            "mentionsourcename", "mentionidentifier", "sentenceid",
            "actor1charoffset", "actor2charoffset", "actioncharoffset",
            "inrawtext", "confidence", "mentiondoclen", "mentiondoctone",
            "mentiondocoriginalreporter", "mentiondocoriginalsource"
        ],
        "gkg": [
            "gkgrecordid", "date", "sourcecollectionidentifier", "sourcecommonname",
            "documentidentifier", "counts", "v2counts", "themes", "v2themes",
            "locations", "v2locations", "persons", "v2persons",
            "organizations", "v2organizations", "tone", "v2tone", "dates",
            "gcam", "sharingimage", "relatedimages", "socialimageembeds",
            "socialvideoembeds", "quotations", "allnames", "amounts",
            "translationinfo"
        ]
    }

    df.columns = COLUMNAS[table]
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
