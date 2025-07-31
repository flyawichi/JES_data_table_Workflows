from datetime import datetime
import pandas as pd

def safe_parse_date(date_str):
    if not date_str or not str(date_str).strip():
        return None
    if "T" in date_str:
        try:
            return datetime.strptime(date_str[:10], "%Y-%m-%d").date()
        except Exception:
            pass
    formats = [
        '%Y-%m-%d',
        '%m/%d/%Y',
        '%d-%b-%Y',
        '%B %d, %Y',
        '%Y/%m/%d',
        '%d/%m/%Y',
    ]
    for fmt in formats:
        try:
            parsed = datetime.strptime(date_str.strip(), fmt)
            return parsed.date()
        except ValueError:
            continue
    return None

def normalize_factored(value):
    v = str(value).strip().lower()
    if v in ("true", "yes", "1"):
        return "Yes"
    elif v in ("false", "no", "0"):
        return "No"
    elif v == "pending":
        return "Pending"
    return "Pending"

def parse_currency(val):
    try:
        fval = float(val)
        if pd.isnull(fval) or fval != fval:  # catch np.nan, float('nan')
            return 0.00
        return round(fval, 2)
    except (TypeError, ValueError):
        return 0.00
