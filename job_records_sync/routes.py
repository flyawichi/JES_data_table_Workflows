from flask import Blueprint, request, jsonify
from db import get_connection
import pandas as pd
import numpy as np

job_records_api = Blueprint('job_records_api', __name__, url_prefix='/job-records')

@job_records_api.route("/sync", methods=["POST"])
def sync_sheet_to_db():
    data = request.json.get("data", [])
    print("JOB RECORDS SYNC ENDPOINT TRIGGERED")

    df = pd.DataFrame(data)
    df.columns = [c.lower() for c in df.columns]

    # Replace missing, dash, and placeholder values with None
    df = df.replace({
        pd.NaT: None, np.nan: None, "NaT": None, "nan": None, "NaN": None,
        "": None, "-": None
    })

    # Explicitly clean all numeric columns in case they slipped through as strings
    numeric_cols = [
        "pay", "detention_amount", "tonu_amount", "driver_assist_amount", "grand_total",
        "proposed_live_miles", "proposed_dead_miles", "actual_live_miles", "actual_dead_miles", "stops"
    ]
    for col in numeric_cols:
        if col in df.columns:
            # Convert blanks or dashes to None, otherwise try to convert to float/int
            def to_num(val):
                if val in [None, "", "-", "NaN", "nan"]:
                    return None
                try:
                    return float(val)
                except (ValueError, TypeError):
                    return None
            df[col] = df[col].apply(to_num)

    # Clean all known date/timestamp columns
    date_cols = ["assignment_date", "start_date", "end_date", "last_updated"]
    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: None if x in [pd.NaT, "NaT", np.nan, "nan", "NaN", ""] else x)

    print("\n==== Pandas DataFrame (Staging Table, BEFORE SQL Push) ====")
    print(df.head(10))
    print("Null counts:\n", df.isnull().sum())
    print("===========================================\n")

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO operations.job_records_input (
                job_id, assignment_date, broker_name, bol_reference,
                description, origin, start_date, destination, end_date,
                route, stops, proposed_live_miles, proposed_dead_miles,
                actual_live_miles, actual_dead_miles, pay, detention_amount,
                tonu_amount, driver_assist_amount, grand_total, rate_confirmation,
                proof_of_delivery, status, dispatch_id, note, row_id, last_updated, source
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (job_id) DO UPDATE SET
                assignment_date = EXCLUDED.assignment_date,
                broker_name = EXCLUDED.broker_name,
                bol_reference = EXCLUDED.bol_reference,
                description = EXCLUDED.description,
                origin = EXCLUDED.origin,
                start_date = EXCLUDED.start_date,
                destination = EXCLUDED.destination,
                end_date = EXCLUDED.end_date,
                route = EXCLUDED.route,
                stops = EXCLUDED.stops,
                proposed_live_miles = EXCLUDED.proposed_live_miles,
                proposed_dead_miles = EXCLUDED.proposed_dead_miles,
                actual_live_miles = EXCLUDED.actual_live_miles,
                actual_dead_miles = EXCLUDED.actual_dead_miles,
                pay = EXCLUDED.pay,
                detention_amount = EXCLUDED.detention_amount,
                tonu_amount = EXCLUDED.tonu_amount,
                driver_assist_amount = EXCLUDED.driver_assist_amount,
                grand_total = EXCLUDED.grand_total,
                rate_confirmation = EXCLUDED.rate_confirmation,
                proof_of_delivery = EXCLUDED.proof_of_delivery,
                status = EXCLUDED.status,
                dispatch_id = EXCLUDED.dispatch_id,
                note = EXCLUDED.note,
                row_id = EXCLUDED.row_id,
                last_updated = EXCLUDED.last_updated,
                source = EXCLUDED.source
        """, (
            row.get("job_id"),
            row.get("assignment_date"),
            row.get("broker_name"),
            row.get("bol_reference"),
            row.get("description"),
            row.get("origin"),
            row.get("start_date"),
            row.get("destination"),
            row.get("end_date"),
            row.get("route"),
            row.get("stops"),
            row.get("proposed_live_miles"),
            row.get("proposed_dead_miles"),
            row.get("actual_live_miles"),
            row.get("actual_dead_miles"),
            row.get("pay"),
            row.get("detention_amount"),
            row.get("tonu_amount"),
            row.get("driver_assist_amount"),
            row.get("grand_total"),
            row.get("rate_confirmation"),
            row.get("proof_of_delivery"),
            row.get("status"),
            row.get("dispatch_id"),
            row.get("note"),
            row.get("row_id"),
            row.get("last_updated"),
            row.get("source", "sheet")
        ))

    conn.commit()
    cur.close()
    conn.close()

    # Optional: Secondary verification (query back latest records)
    conn2 = get_connection()
    cur2 = conn2.cursor()
    cur2.execute("""
        SELECT job_id, assignment_date, broker_name, bol_reference, description, origin, start_date, destination, end_date,
               route, stops, proposed_live_miles, proposed_dead_miles, actual_live_miles, actual_dead_miles, pay,
               detention_amount, tonu_amount, driver_assist_amount, grand_total, rate_confirmation, proof_of_delivery,
               status, dispatch_id, note, row_id, last_updated, source
        FROM operations.job_records_input
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

