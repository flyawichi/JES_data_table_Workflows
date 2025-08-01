from flask import Blueprint, request, jsonify
from db import get_connection
from datetime import datetime
import pandas as pd
from ar_db_sync.utils import safe_parse_date, parse_currency  # No factored/actual_payment for AP

ap_api = Blueprint('ap_api', __name__, url_prefix='/ap')

@ap_api.route("/sync", methods=["POST"])
def sync_sheet_to_db():
    data = request.json.get("data", [])
    print("AP SYNC ENDPOINT TRIGGERED")

    df = pd.DataFrame(data)
    df.columns = [c.lower() for c in df.columns]

    for col in ["completion_date", "expected_payment_date", "payment_date"]:
        if col in df.columns:
            df[col] = df[col].apply(safe_parse_date)

    print("\n==== Pandas DataFrame (Staging Table, BEFORE SQL Push) ====")
    print(df.head(10))
    print("Null counts:\n", df.isnull().sum())
    print("===========================================\n")

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO accounts_payable (
                completion_date, vendor_id, pay_to,
                transaction_id, transaction_descriptions, amount,
                pay_cycle, expected_payment_date, payment_date,
                status, last_updated, source
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (transaction_id) DO UPDATE SET
                completion_date = EXCLUDED.completion_date,
                vendor_id = EXCLUDED.vendor_id,
                pay_to = EXCLUDED.pay_to,
                transaction_descriptions = EXCLUDED.transaction_descriptions,
                amount = EXCLUDED.amount,
                pay_cycle = EXCLUDED.pay_cycle,
                expected_payment_date = EXCLUDED.expected_payment_date,
                payment_date = EXCLUDED.payment_date,
                status = EXCLUDED.status,
                last_updated = EXCLUDED.last_updated,
                source = EXCLUDED.source
        """, (
            row.get("completion_date"),
            row.get("vendor_id"),
            row.get("pay_to"),
            row.get("transaction_id"),
            row.get("transaction_descriptions"),
            parse_currency(row.get("amount")),
            row.get("pay_cycle"),
            row.get("expected_payment_date"),
            row.get("payment_date"),
            row.get("status"),
            datetime.utcnow(),
            "sheet"
        ))

    conn.commit()
    cur.close()
    conn.close()

    # Secondary verification: Query back and print
    conn2 = get_connection()
    cur2 = conn2.cursor()
    cur2.execute("""
        SELECT completion_date, vendor_id, pay_to, transaction_id,
               transaction_descriptions, amount, pay_cycle,
               expected_payment_date, payment_date, status,
               last_updated, source
        FROM accounts_payable
        ORDER BY last_updated DESC
        LIMIT 10
    """)
    result_rows = cur2.fetchall()
    columns = [desc[0] for desc in cur2.description]
    df_after = pd.DataFrame(result_rows, columns=columns)
    print("\n==== DataFrame AFTER SQL Push (Pulled from DB) ====")
    print(df_after)
    print("Null counts after push:\n", df_after.isnull().sum())
    print("===========================================\n")
    cur2.close()
    conn2.close()

    return jsonify({"status": "success", "rows_synced": len(df)})
