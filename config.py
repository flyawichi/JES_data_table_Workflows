# Fill in your connection details
PG_CONFIG = {
    'dbname': 'jesadmin',
    'user': 'postgres',
    'password': '24652465',
    'host': 'localhost',
    'port': 5432
}

SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://user:24652465@localhost:5432/jesadmin"

# For Google Sheets sync:
JOB_RECORDS_SHEET_NAME = "job_records"    # <-- This is your sheet/tab name
GSHEETS_KEY_PATH = r"C:\Users\Owner\CodeRepos\JES Data Table Sync\jes-gsheets-service-account-123abc456def.json"
