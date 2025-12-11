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

SERVICENOW_USER = "admin"   # If needed, you can make this a secret too
SERVICENOW_URL = (
    "https://dev181336.service-now.com/api/now/table/x_1854014_incide_0_incidents"
    "?sysparm_display_value=all"
)

# Initialize Smartsheet client
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
# 1) Fetch Latest ServiceNow Record
# ---------------------------------------------------------
def fetch_latest_servicenow_record():
    url = f"{SERVICENOW_INSTANCE}/api/now/table/{SERVICENOW_TABLE}"
    
    # Get more rows so sorting makes sense
    params = {
        "sysparm_limit": 50,             # Pull enough rows
        "sysparm_display_value": "true"  # So client_name is readable
    }

    resp = requests.get(url, auth=(SERVICENOW_USER, SERVICENOW_PWD), params=params)
    resp.raise_for_status()

    result = resp.json().get("result", [])
    if not result:
        raise Exception("No records returned from ServiceNow")

    # Sort by Client Name descending
    sorted_records = sorted(
        result,
        key=lambda x: (x.get("client_name") or "").lower(),
        reverse=True
    )

    # Take the first after sorting
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

        val = dotted_get(latest_record, sn_key)

        if sm_col_title == "Requested Date":
            val = normalize_date(val)

        if sm_col_title == "Project Created":
            if str(val).lower() in ("true", "1", "yes"):
                val = True
            elif str(val).lower() in ("false", "0", "no"):
                val = False

        if val is None:
            val = ""

        cells.append(
            smartsheet.models.Cell({
                "column_id": col_id,
                "value": val
            })
        )

    # Guarantee primary column
    client_name_col_id = col_map.get("Client Name")
    if client_name_col_id and not any(c.column_id == client_name_col_id for c in cells):
        cells.append(
            smartsheet.models.Cell({
                "column_id": client_name_col_id,
                "value": "Unknown Client"
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
