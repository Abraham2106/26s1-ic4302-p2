
## Superset (Visualizaciones y Datasets)
La base de datos de metadata de Superset se almacena localmente usando SQLite para persistencia rápida en desarrollo.

### Persistencia y Volúmenes
- Los datos de Superset se almacenan localmente en la carpeta `./p2/superset_home` la cual está mapeada al contenedor en `docker-compose.yml`.
- Para **mantener la configuración** y los datasets creados entre reinicios, simplemente deje intacta esta carpeta.
- Para **resetear Superset desde cero** (reinicializar base de datos, recrear usuario administrador y volver a correr el script de datasets), detenga el servicio y borre esta carpeta:
  ```bash
  docker compose stop superset
  rm -rf ./p2/superset_home/*
  docker compose up -d superset
  ```

### Script de Inicialización Automática
Al levantar el contenedor de Superset, el entrypoint ejecuta el script `/app/superset/init/superset-init.sh` que realiza:
1. Migraciones de base de datos (`superset db upgrade`).
2. Creación del usuario administrador (`admin` / `admin`).
3. Inicialización de roles de Superset (`superset init`).
4. Configuración programática del conector a **Trino** y la creación automática de los **18 datasets** correspondientes a las colecciones de MongoDB (`create_datasets.py`).
5. Inicio del servidor web (`gunicorn`).