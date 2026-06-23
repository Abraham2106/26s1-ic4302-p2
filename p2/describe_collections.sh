#!/bin/bash
tables=$(docker exec trino-coordinator trino --execute "SHOW TABLES FROM mongodb.gdelt" | tr -d '"' | tr -d '\r' | awk 'NF')

for table in $tables; do
  echo "====== $table ======"
  docker exec trino-coordinator trino --execute "DESCRIBE mongodb.gdelt.$table"
  echo ""
done
