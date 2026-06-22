SECRET_KEY = "gdelt-superset-dev-key-change-in-prod"
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# Superset crea su propia metadata DB. SQLite es suficiente para dev.
# En producción usar PostgreSQL.
