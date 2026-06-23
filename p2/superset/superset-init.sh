#!/bin/bash
set -e
superset db upgrade
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@superset.local \
  --password admin
superset init
