import json
import math
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests


# -----------------------------
# CONFIG
# -----------------------------
SHEET_ID = "8815098509348740"
SHEET_URL = f"https://api.smartsheet.com/2.0/sheets/{SHEET_ID}"

ROWS_POST_URL = (
    f"https://api.smartsheet.com/2.0/sheets/{SHEET_ID}/rows"
    f"?toTop=true&overrideValidation=true&strict=false&allowPartialSuccess=true"
)

TOKEN = "Token"   # put your real token
VERIFY_ARG = True

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}


# -----------------------------
# ServiceNow Pull Function
# -----------------------------
def pull_servicenow_data():
    url = "https://dev181336.service-now.com/api/now/table/x_1854014_incide_0_incidents?sysparm_display_value=all"

    user = "admin"
    pwd = "pwd"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    response = requests.get(url, auth=(user, pwd), headers=headers)

    if response.status_code != 200:
        print("Status:", response.status_code, "Error:", response.text)
        raise RuntimeError("ServiceNow error")

    return response.json()


# -----------------------------
# FIELD MAP (ServiceNow → Smartsheet)
# -----------------------------
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

DISPLAY_FALLBACKS = {
    "short_description.display_value": "short_description",
    "city.display_value": "city",
    "client_email.display_value": "client_email",
    "active.display_value": "active",
    "issue_raised_date.display_value": "issue_raised_date",
    "company.display_value": "company",
    "category.display_value": "category",
    "department.display_value": "department",
    "subcategory.display_value": "subcategory",
    "client_name.display_value": "client_name",
}

UNIQUE_KEY_TITLES = ["Short Description", "Client Email", "Requested Date"]


# -----------------------------
# HELPERS
# -----------------------------
def dotted_get(obj: Dict[str, Any], dotted_key: str) -> Any:
    cur = obj
    for part in dotted_key.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def resolve_value(row: Dict[str, Any], source_key: str) -> Any:
    val = dotted_get(row, source_key)
    if val is not None:
        return val

    if source_key in row:
        return row[source_key]

    fallback = DISPLAY_FALLBACKS.get(source_key)
    if fallback:
        base = row.get(fallback)
        if isinstance(base, dict):
            for k in ("display_value", "value", "text", "name", "label"):
                if k in base and base[k]:
                    return base[k]
        return base

    return None


def normalize_for_column(value: Any, smartsheet_type: Optional[str]) -> Any:
    if value is None:
        return None

    if smartsheet_type == "DATE":
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

    if smartsheet_type in ("CHECKBOX", "BOOLEAN"):
        if str(value).lower() in ("true", "1", "yes"):
            return True
        if str(value).lower() in ("false", "0", "no"):
            return False

    return value


def get_sheet_columns():
    resp = requests.get(SHEET_URL, headers=HEADERS, verify=VERIFY_ARG)
    resp.raise_for_status()
    data = resp.json()

    columns = data["columns"]
    title_to_id = {c["title"]: c["id"] for c in columns}
    title_to_type = {c["title"]: c["type"] for c in columns}

    return title_to_id, title_to_type


def build_cells(row, title_to_id, title_to_type):
    cells = []
    for sn_key, sm_title in FIELD_MAP.items():
        col_id = title_to_id.get(sm_title)
        if not col_id:
            continue

        raw = resolve_value(row, sn_key)
        norm = normalize_for_column(raw, title_to_type.get(sm_title))

        cell = {"columnId": col_id}
        if norm is not None:
            cell["value"] = norm

        cells.append(cell)

    return cells


def get_last_record(data):
    result = data.get("result", [])
    if not result:
        raise RuntimeError("No records in ServiceNow response")

    def get_dt(rec):
        dv = dotted_get(rec, "issue_raised_date.display_value")
        if dv:
            try:
                return datetime.strptime(dv[:19], "%Y-%m-%d %H:%M:%S")
            except:
                return None
        return None

    result_with_dt = [(r, get_dt(r)) for r in result]
    sorted_records = sorted(result_with_dt, key=lambda x: x[1] or datetime.min)

    return sorted_records[-1][0]


def post_row(cells):
    payload = {"rows": [{"cells": cells}]}

    resp = requests.post(ROWS_POST_URL, headers=HEADERS, json=payload, verify=VERIFY_ARG)
    resp.raise_for_status()

    return resp.json()


# -----------------------------
# MAIN
# -----------------------------
def main():
    # Pull from ServiceNow
    data = pull_servicenow_data()

    # Extract last record
    latest = get_last_record(data)

    # Fetch sheet column meta
    title_to_id, title_to_type = get_sheet_columns()

    # Build row
    cells = build_cells(latest, title_to_id, title_to_type)

    # Post to Smartsheet
    result = post_row(cells)
    print("Inserted successfully:", json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
