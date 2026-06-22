import os
import sys
import logging

from superset.app import create_app
app = create_app()
app.app_context().push()

from flask_appbuilder.security.sqla.models import User
from superset import db, security_manager
from superset.models.core import Database
from superset.connectors.sqla.models import SqlaTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("superset_init_script")

def run():
    logger.info("Starting programmatic configuration of Superset...")

    trino_uri = "trino://admin@trino-coordinator:8080/mongodb/gdelt"
    db_name = "Trino"
    
    database = db.session.query(Database).filter_by(database_name=db_name).first()
    if not database:
        logger.info(f"Creating database connection to Trino with URI: {trino_uri}")
        database = Database(database_name=db_name, sqlalchemy_uri=trino_uri)
        db.session.add(database)
        db.session.commit()
        database = db.session.query(Database).filter_by(database_name=db_name).first()
    else:
        logger.info(f"Database connection 'Trino' already exists (ID: {database.id}). Updating URI if necessary.")
        if database.sqlalchemy_uri != trino_uri:
            database.sqlalchemy_uri = trino_uri
            db.session.add(database)
            db.session.commit()

    collections = [
        "cameo_region_mundo",
        "cobertura_mediatica_pais",
        "conflictos_pares_paises",
        "correlacion_avgtone_fuentes",
        "correlacion_tono_conflicto_ventana",
        "diversidad_fuentes_pais",
        "escalada_eventos_1h",
        "eventos_positivos_negativos",
        "frecuencia_conflictos_etnia",
        "grafo_interacciones_paises",
        "heatmap_conflictos",
        "matriz_interaccion_actores",
        "tendencia_sentimiento_pais",
        "tono_conflicto_ventana",
        "top10_paises_eventos_dia",
        "top10_paises_menos_noticiosos",
        "top_organizaciones_global_dia",
        "top_temas_continente_anio"
    ]

    schema = "gdelt"
    succeeded = 0

    for col in collections:
        table = db.session.query(SqlaTable).filter_by(
            database_id=database.id,
            schema=schema,
            table_name=col
        ).first()

        if not table:
            logger.info(f"Creating dataset for table: {schema}.{col}")
            try:
                table = SqlaTable(
                    table_name=col,
                    schema=schema,
                    database_id=database.id,
                    database=database
                )
                db.session.add(table)
                db.session.commit()
                succeeded += 1
            except Exception as e:
                db.session.rollback()
                logger.error(f"Failed to create dataset for '{col}': {e}")
        else:
            logger.info(f"Dataset for table '{schema}.{col}' already exists.")
            succeeded += 1

    logger.info(f"Superset programmatic initialization complete! {succeeded}/{len(collections)} datasets configured.")

if __name__ == "__main__":
    run()
