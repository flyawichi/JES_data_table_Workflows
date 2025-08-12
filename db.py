import psycopg2
from config import PG_CONFIG, SQLALCHEMY_DATABASE_URI
from sqlalchemy import create_engine

def get_connection():
    return psycopg2.connect(**PG_CONFIG)

# Add this for SQLAlchemy-powered code (sync modules, pandas.to_sql, etc.)
engine = create_engine(SQLALCHEMY_DATABASE_URI)
