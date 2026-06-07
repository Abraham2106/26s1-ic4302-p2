from pathlib import Path
import requests
import os

URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

def obtener_datos():

    Path("archivos_zip").mkdir(exist_ok=True)

    # extraer texto del link
    texto_indice = requests.get(URL)
    texto_indice.raise_for_status()

    # mira los 3 archivos y los descarga
    for linea in texto_indice.text.splitlines():
        peso, codigo, enlace = linea.split()
        nombre = enlace.split("/")[-1]
        destino = Path("archivos_zip") / nombre

        # evitar descargar dos veces
        if destino.exists():
            continue

        archivo = requests.get(enlace)
        archivo.raise_for_status()

        with open(destino, "wb") as f:
            f.write(archivo.content)

        #print(f"Descargado: {nombre}") para debug

# puede mejorar, elimina todos menos los ultimos 12 archivos
def eliminar_datos():
    nombres = sorted(os.listdir("archivos_zip"))

    eliminar = max(0, len(nombres) - 12)

    for i in range(eliminar):
        os.remove(f"archivos_zip/{nombres[i]}")
