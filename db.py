import psycopg2
from config import PG_CONFIG

def get_connection():
    return psycopg2.connect(**PG_CONFIG)
