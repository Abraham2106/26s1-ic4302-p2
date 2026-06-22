#!/bin/bash
set -e

echo "actualiza db"
superset db upgrade

echo "crea admin"
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@superset.local \
  --password admin || echo "Admin user already exists or failed to create."

echo "inicializa superset"
superset init

echo "crea trino y datasets"
python /app/superset/init/create_datasets.py

echo "inicia superset"
gunicorn \
  --bind "0.0.0.0:8088" \
  --access-logfile - \
  --error-logfile - \
  --workers 1 \
  --worker-class gthread \
  --threads 4 \
  --timeout 60 \
  --limit-request-line 0 \
  --limit-request-field_size 0 \
  "superset.app:create_app()"
