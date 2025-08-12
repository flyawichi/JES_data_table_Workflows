import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import config
import numpy as np

def fetch_google_sheet(sheet_name, worksheet_index=0):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        config.GSHEETS_KEY_PATH, scope
    )
    gc = gspread.authorize(creds)
    sheet = gc.open(sheet_name).get_worksheet(worksheet_index)
    records = sheet.get_all_records()
    return pd.DataFrame(records)

def clean_and_format_job_records(df):
    df.rename(columns={
        'Job_ID': 'job_id',
        'Assingment Date': 'assignment_date',
        'Broker': 'broker_name',
        'BOL Reference #': 'bol_reference',
        'Desciption': 'description',
        'From': 'origin',
        'Start Date': 'start_date',
        'To': 'destination',
        'End Date': 'end_date',
        'Route': 'route',
        'Stops': 'stops',
        'Proposed Live Miles': 'proposed_live_miles',
        'Proposed Dead Miles': 'proposed_dead_miles',
        'Actual Live Miles': 'actual_live_miles',
        'Actual Dead Miles': 'actual_dead_miles',
        'Pay($)': 'pay',
        'Detention': 'detention_amount',
        'Tonu': 'tonu_amount',
        'Driver Assist': 'driver_assist_amount',
        'Grand Total': 'grand_total',
        'Rate Confirmation': 'rate_confirmation',
        'Proof Of Delvery': 'proof_of_delivery',
        'Status': 'status',
        'Dispatch ID': 'dispatch_id',
        'Note': 'note',
        'Row_ID': 'row_id',
        'Last_Updated': 'last_updated',
        'Source': 'source'
    }, inplace=True)

    # Format date columns
    date_cols = ['assignment_date', 'start_date', 'end_date', 'last_updated']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    df.replace({np.nan: None, '': None}, inplace=True)
    return df
