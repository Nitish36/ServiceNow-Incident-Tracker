import os
import json
import math
from typing import Any, Dict, List, Tuple, Optional
from datetime import datetime

import requests

try:
    import pandas as pd
except ImportError:
    pd = None


# -----------------------------
# Config
# -----------------------------
SHEET_ID = "[Sheet ID]"
SHEET_URL = f"https://api.smartsheet.com/2.0/sheets/{SHEET_ID}"

# Insert at TOP; enable override for picklist/type mismatches (if needed)
ROWS_POST_URL = (
    f"https://api.smartsheet.com/2.0/sheets/{SHEET_ID}/rows"
    f"?toTop=true&overrideValidation=true&strict=false&allowPartialSuccess=true"
)

TOKEN = "[Token]"
CA_BUNDLE = os.getenv("REQUESTS_CA_BUNDLE") or os.getenv("SSL_CERT_FILE")
VERIFY_ARG = CA_BUNDLE if CA_BUNDLE else True
PROXIES = {"http": os.getenv("HTTP_PROXY"), "https": os.getenv("HTTPS_PROXY")}

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "smartsheet-integration-source": "AI,SampleOrg,My-AI-Connector-v2",
}

# ServiceNow -> Smartsheet column titles (must match sheet titles exactly)
FIELD_MAP: Dict[str, str] = {
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

# If your ServiceNow payload doesn’t include display_value, we fallback:
DISPLAY_FALLBACKS: Dict[str, str] = {
    "short_description.display_value": "short_description",
    "city.display_value": "city",
    "client_email.display_value": "client_email",
    "active.display_value": "active",
    "issue_raised_date.display_value": "issue_raised_date",  # adjust if needed
    "company.display_value": "company",
    "category.display_value": "category",
    "department.display_value": "department",
    "subcategory.display_value": "subcategory",
    "client_name.display_value": "client_name",
}

# Composite key to detect duplicates in Smartsheet
UNIQUE_KEY_TITLES = ["Short Description", "Client Email", "Requested Date"]


# -----------------------------
# Helpers
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
    # dotted traversal first
    val = dotted_get(row, source_key)
    if val is not None:
        return val
    # literal key (flattened dicts)
    if source_key in row:
        return row[source_key]
    # base fallback
    base_key = DISPLAY_FALLBACKS.get(source_key)
    if base_key:
        base_val = row.get(base_key)
        if isinstance(base_val, dict):
            for k in ("display_value", "value", "text", "name", "label"):
                if k in base_val and base_val[k] not in (None, ""):
                    return base_val[k]
            return base_val
        return base_val
    return None


def normalize_for_column(value: Any, smartsheet_type: Optional[str]) -> Any:
    # Handle None/empty
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    if isinstance(value, float):
        try:
            if math.isnan(value):
                return None
        except Exception:
            pass

    if smartsheet_type == "DATE":
        fmts = [
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%S%z",
        ]
        if isinstance(value, str):
            for fmt in fmts:
                try:
                    dt = datetime.strptime(value, fmt)
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue
        if hasattr(value, "strftime"):
            return value.strftime("%Y-%m-%d")

    if smartsheet_type in ("CHECKBOX", "BOOLEAN"):
        if isinstance(value, str):
            v = value.strip().lower()
            if v in ("true", "yes", "y", "1"):
                return True
            if v in ("false", "no", "n", "0"):
                return False
        if isinstance(value, (bool, int)):
            return bool(value)

    # others: pass through
    return value


def get_sheet_columns() -> Tuple[Dict[str, int], Dict[str, str], Dict[int, str]]:
    
    
    resp = requests.get(SHEET_URL, headers=HEADERS, verify=VERIFY_ARG, proxies=PROXIES)
    resp.raise_for_status()
    meta = resp.json()
    columns = meta.get("columns", [])
    title_to_id = {c["title"]: c["id"] for c in columns}
    title_to_type = {c["title"]: c.get("type") for c in columns}
    id_to_title = {c["id"]: c["title"] for c in columns}
    return title_to_id, title_to_type, id_to_title


def build_cells(
    row: Dict[str, Any],
    title_to_id: Dict[str, int],
    title_to_type: Dict[str, str],
) -> List[Dict[str, Any]]:
    
    cells: List[Dict[str, Any]] = []
    for sn_key, sm_title in FIELD_MAP.items():
        col_id = title_to_id.get(sm_title)
        if col_id is None:
            print(f"[WARN] Column not found in Smartsheet: {sm_title}")
            continue
        raw = resolve_value(row, sn_key)
        norm = normalize_for_column(raw, title_to_type.get(sm_title))
        print(f"[MAP] {sn_key} -> {sm_title} | raw={repr(raw)} | norm={repr(norm)}")
        cell = {"columnId": col_id}
        if norm is not None:
            cell["value"] = norm
        cells.append(cell)
    return cells


def to_key_value_from_row(row: Dict[str, Any], title_to_type: Dict[str, str]) -> Dict[str, Any]:
    
    out = {}
    for sn_key, sm_title in FIELD_MAP.items():
        if sm_title in UNIQUE_KEY_TITLES:
            raw = resolve_value(row, sn_key)
            norm = normalize_for_column(raw, title_to_type.get(sm_title))
            out[sm_title] = norm
    return out


def read_sheet_rows_for_keys(
    title_to_id: Dict[str, int],
) -> List[Dict[str, Any]]:
    
    url = f"{SHEET_URL}?include=objectValue&level=2"
    resp = requests.get(url, headers=HEADERS, verify=VERIFY_ARG, proxies=PROXIES)
    resp.raise_for_status()
    sheet = resp.json()
    rows = sheet.get("rows", [])
    id_to_title = {v: k for k, v in title_to_id.items()}

    existing = []
    for r in rows:
        kv = {}
        for c in r.get("cells", []):
            title = id_to_title.get(c.get("columnId"))
            if not title or title not in UNIQUE_KEY_TITLES:
                continue
            # prefer value over displayValue for matching
            val = c.get("value")
            if val is None:
                val = c.get("displayValue")
            kv[title] = val
        if kv:
            existing.append(kv)
    return existing


def same_key(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    
    for t in UNIQUE_KEY_TITLES:
        va = a.get(t)
        vb = b.get(t)
        if va is None and vb is None:
            continue
        if va is None or vb is None:
            return False
        sa = str(va).strip().lower()
        sb = str(vb).strip().lower()
        if sa != sb:
            return False
    return True


def post_row(cells: List[Dict[str, Any]]) -> Dict[str, Any]:
    payload = {"rows": [{"cells": cells}]}
    print("\nPayload to POST:\n", json.dumps(payload, indent=2))
    resp = requests.post(ROWS_POST_URL, headers=HEADERS, json=payload, verify=VERIFY_ARG, proxies=PROXIES)
    print("POST status:", resp.status_code)
    print("Response (first 400 chars):", resp.text[:400])
    resp.raise_for_status()
    return resp.json()


def get_last_record(data: Any) -> Dict[str, Any]:
    
    # Extract list of items (records)
    records: List[Dict[str, Any]] = []
    if isinstance(data, list):
        records = [x for x in data if isinstance(x, dict)]
    elif pd is not None and isinstance(data, pd.DataFrame):
        if not data.empty:
            records = [row.to_dict() for _, row in data.iterrows()]
    elif isinstance(data, dict):
        for key in ("result", "records", "data", "items"):
            if key in data and isinstance(data[key], list):
                records = [x for x in data[key] if isinstance(x, dict)]
                break
        if not records and data:
            # dict keyed by IDs, use values
            records = [v for v in data.values() if isinstance(v, dict)]

    if not records:
        raise RuntimeError("No records found in ServiceNow payload.")

    # Choose latest by issue_raised_date.[display_value] or fallback to order
    def parse_dt(x: Any) -> Optional[datetime]:
        if x is None or (isinstance(x, str) and x.strip() == ""):
            return None
        fmts = [
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%S%z",
        ]
        if isinstance(x, str):
            for fmt in fmts:
                try:
                    return datetime.strptime(x, fmt)
                except ValueError:
                    continue
        return None

    # Try to get time per record
    def get_time(rec: Dict[str, Any]) -> Optional[datetime]:
        dv = dotted_get(rec, "issue_raised_date.display_value")
        if dv is None and "issue_raised_date.display_value" in rec:
            dv = rec["issue_raised_date.display_value"]
        if dv is None:
            dv = rec.get("issue_raised_date")
        return parse_dt(dv)

    # If any timestamps present, pick max; else pick last by order
    times = [(i, get_time(r)) for i, r in enumerate(records)]
    if any(t for _, t in times):
        latest_idx, _ = max(times, key=lambda it: it[1] or datetime.min)
        return records[latest_idx]
    else:
        return records[-1]


# -----------------------------
# Main
# -----------------------------
def main():
    if not TOKEN or TOKEN.startswith("REPLACE_WITH"):
        raise RuntimeError("Set SMARTSHEET_TOKEN env var or replace TOKEN with your actual API token.")

    # Resolve sheet columns
    title_to_id, title_to_type, id_to_title = get_sheet_columns()
    print("Columns:", json.dumps(title_to_id, indent=2))
    print("Types:", json.dumps(title_to_type, indent=2))

    # Pull ServiceNow data
    from test import pull_servicenow_data
    data = pull_servicenow_data()
    print("ServiceNow data type:", type(data).__name__)

    # Locate the latest record
    latest = get_last_record(data)
    print("\nLatest ServiceNow record preview:\n", json.dumps(latest, indent=2)[:1200])

    # Build composite key from latest
    latest_key = to_key_value_from_row(latest, title_to_type)
    print("\nLatest composite key:", json.dumps(latest_key, indent=2))

    # Read existing keys from sheet and compare for dedup
    existing_keys = read_sheet_rows_for_keys(title_to_id)
    print(f"\nExisting rows (keys only): {len(existing_keys)}")
    duplicate = any(same_key(latest_key, ek) for ek in existing_keys)

    if duplicate:
        print("\n[SKIP] A row with the same composite key already exists in Smartsheet.")
        return

    # Build cells and POST
    cells = build_cells(latest, title_to_id, title_to_type)

    # Guard against empty insert
    if not any(("value" in c and c["value"] is not None) for c in cells):
        raise RuntimeError("All mapped values empty; nothing to insert. Check mapping and ServiceNow payload.")

    posted = post_row(cells)
    print("\nPosted result:\n", json.dumps(posted, indent=2))


if __name__ == "__main__":
    main()
