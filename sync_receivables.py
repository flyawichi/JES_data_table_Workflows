import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from uuid import uuid4

# PostgreSQL Config
PG_CONFIG = {
    'dbname': 'jesadmin',
    'user': 'postgres',
    'password': '24652465',
    'host': 'localhost',
    'port': 5432
}

# Google Sheets Config
SPREADSHEET_NAME = "Receivables"
SHEET_TAB = "Receivables"

def get_pg_conn():
    return psycopg2.connect(**PG_CONFIG)

def fetch_pg_data():
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM accounts_receivables;")
    rows = cur.fetchall()
    headers = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return [dict(zip(headers, row)) for row in rows]

def fetch_sheet_data(sheet):
    values = sheet.get_all_values()
    headers = values[0] + ['row_id', 'last_updated', 'source']
    data = values[1:]
    results = []
    for row in data:
        row_dict = dict(zip(headers, row + [None]*(len(headers) - len(row))))
        results.append(row_dict)
    return results

def sync():
    # Auth for Google Sheets
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)
    sheet = client.open(SPREADSHEET_NAME).worksheet(SHEET_TAB)

    sheet_data = fetch_sheet_data(sheet)
    pg_data = fetch_pg_data()

    sheet_dict = {row['row_id']: row for row in sheet_data if row['row_id']}
    pg_dict = {row['row_id']: row for row in pg_data}

    conn = get_pg_conn()
    cur = conn.cursor()

    # Sync from sheet to DB
    for row in sheet_data:
        rid = row.get('row_id')
        now = datetime.utcnow().isoformat()
        if not rid:
            rid = str(uuid4())
            row['row_id'] = rid
            row['last_updated'] = now
            row['source'] = 'sheet'
            cur.execute("""
                INSERT INTO accounts_receivables (
                    row_id, completion_date, vendor_id, bill_to,
                    transaction_id, transaction_descriptions,
                    amount, pay_cycle, expected_payment_date,
                    payment_date, actual_payment, factored,
                    status, last_updated, source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                rid,
                row['Completion Date'] or None,
                row['VendorID'], row['Bill To'], row['TransactionID'],
                row['Transaction Descriptions'], row['Amount'],
                row['Pay Cycle'], row['Exp. Payment Date'],
                row['Payment Date'], row['Act. Payment'],
                row['Factored'].lower() == 'true' if row['Factored'] else None,
                row['Status'], now, 'sheet'
            ))
        elif rid in pg_dict:
            sheet_time = row['last_updated']
            db_time = pg_dict[rid]['last_updated']
            if sheet_time > db_time.isoformat():
                # Update PostgreSQL
                cur.execute("""
                    UPDATE accounts_receivables
                    SET status = %s, last_updated = %s, source = %s
                    WHERE row_id = %s
                """, (row['Status'], now, 'sheet', rid))

    # Sync from DB to Sheet
    for rid, row in pg_dict.items():
        if rid not in sheet_dict:
            new_row = [
                row['completion_date'], row['vendor_id'], row['bill_to'],
                row['transaction_id'], row['transaction_descriptions'],
                row['amount'], row['pay_cycle'], row['expected_payment_date'],
                row['payment_date'], row['actual_payment'], str(row['factored']),
                row['status'], row['row_id'], row['last_updated'].isoformat(), 'db'
            ]
            sheet.append_row(new_row)

    conn.commit()
    cur.close()
    conn.close()
    print("Sync complete.")

if __name__ == '__main__':
    sync()
