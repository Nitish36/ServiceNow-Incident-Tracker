import os
import requests
import smartsheet
from datetime import datetime
from typing import Any, Optional, Dict


# ---------------------------------------------------------
# CONFIG — pulled from GitHub Actions environment variables
# ---------------------------------------------------------
SMARTSHEET_TOKEN = os.getenv("SS_TOKEN")
SERVICENOW_PWD = os.getenv("SN_TOKEN")

SHEET_ID = 8815098509348740

SERVICENOW_USER = "admin"   # Make this a secret if you prefer
SERVICENOW_URL = (
    "https://dev181336.service-now.com/api/now/table/x_1854014_incide_0_incidents"
    # removed sysparm_display_value here because we control it via params
)

# Initialize Smartsheet client
if not SMARTSHEET_TOKEN:
    raise RuntimeError("Missing SMARTSHEET_TOKEN (SS_TOKEN) environment variable.")
smartsheet_client = smartsheet.Smartsheet(SMARTSHEET_TOKEN)
smartsheet_client.errors_as_exceptions(True)


# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------
def dotted_get(obj: Dict[str, Any], dotted_key: str) -> Any:
    cur = obj
    for part in dotted_key.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def extract_display_string(value: Any) -> str:
    """
    Given a ServiceNow field value that may be a dict like {'display_value': 'X'},
    or a raw string, return a safe string (or empty string).
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for k in ("display_value", "value", "text", "name", "label"):
            v = value.get(k)
            if v not in (None, ""):
                return str(v)
    # fallback to str of value
    try:
        return str(value)
    except Exception:
        return ""


def normalize_date(value: str) -> Optional[str]:
    if not value:
        return None

    fmts = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
    ]

    for f in fmts:
        try:
            return datetime.strptime(value, f).strftime("%Y-%m-%d")
        except:
            pass

    return None


# ---------------------------------------------------------
# 1) Fetch Latest ServiceNow Record (sorted by client_name desc)
# ---------------------------------------------------------
def fetch_latest_servicenow_record():
    url = f"{SERVICENOW_URL}"
    
    # Pull a batch so sorting works. Increase sysparm_limit if you expect more rows.
    params = {
        "sysparm_limit": 200,
        "sysparm_display_value": "true"  # so display_value fields are present
    }

    resp = requests.get(url, auth=(SERVICENOW_USER, SERVICENOW_PWD), params=params)
    resp.raise_for_status()

    result = resp.json().get("result", [])
    if not result:
        raise Exception("No records returned from ServiceNow")

    # helper to get client_name as a normalized string (for sorting)
    def client_name_key(rec):
        # Prefer display_value path if available
        name_val = dotted_get(rec, "client_name.display_value")
        if name_val is None:
            # fallback to raw client_name field
            name_val = rec.get("client_name")
        # normalize whatever we have to a string
        name_str = extract_display_string(name_val)
        return name_str.lower()  # case-insensitive

    # Sort by Client Name descending and pick first
    sorted_records = sorted(result, key=client_name_key, reverse=True)
    latest = sorted_records[0]
    return latest


# ---------------------------------------------------------
# 2) Build Row for Smartsheet
# ---------------------------------------------------------
FIELD_MAP = {
    "short_description.display_value": "Short Description",
    "city.display_value": "City",
    "client_email.display_value": "Client Email",
    "active.display_value": "Project Created",
    "issue_raised_date.display_value": "Requested Date",
    "company.display_value": "Company",
    "category.display_value": "Category",
    "department.display_value": "Department",
    "subcategory.display_value": "Subcategory",
    "client_name.display_value": "Client Name",
}

def build_row(latest_record):
    sheet = smartsheet_client.Sheets.get_sheet(SHEET_ID)
    col_map = {col.title: col.id for col in sheet.columns}

    cells = []

    for sn_key, sm_col_title in FIELD_MAP.items():
        col_id = col_map.get(sm_col_title)
        if not col_id:
            continue

        raw_val = dotted_get(latest_record, sn_key)
        # if dotted_get returned None, maybe the field is directly present (fallback)
        if raw_val is None:
            raw_val = latest_record.get(sn_key.split(".")[0])  # try base key

        # Convert to appropriate types / formats
        if sm_col_title == "Requested Date":
            # raw_val might be a string or dict; extract string then normalize
            raw_str = extract_display_string(raw_val)
            val = normalize_date(raw_str)
        elif sm_col_title == "Project Created":
            val_str = extract_display_string(raw_val).lower()
            if val_str in ("true", "1", "yes"):
                val = True
            elif val_str in ("false", "0", "no"):
                val = False
            else:
                val = False if raw_val in (None, "", False) else True
        else:
            val = extract_display_string(raw_val)

        # Avoid None — SDK prefers actual value types
        if val is None:
            val = ""

        cells.append(
            smartsheet.models.Cell({
                "column_id": col_id,
                "value": val
            })
        )

    # Guarantee primary column (Client Name) has a visible value
    client_name_col_id = col_map.get("Client Name")
    if client_name_col_id and not any(c.column_id == client_name_col_id for c in cells):
        # try to extract from record
        client_name_val = extract_display_string(dotted_get(latest_record, "client_name.display_value") or latest_record.get("client_name"))
        if not client_name_val:
            client_name_val = "Unknown Client"
        cells.append(
            smartsheet.models.Cell({
                "column_id": client_name_col_id,
                "value": client_name_val
            })
        )

    row = smartsheet.models.Row()
    row.to_top = True
    row.cells = cells
    return row


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    latest = fetch_latest_servicenow_record()
    row = build_row(latest)

    resp = smartsheet_client.Sheets.add_rows(SHEET_ID, [row])
    print("Inserted row:", resp)


if __name__ == "__main__":
    main()
