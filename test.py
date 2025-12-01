import json

import requests
import pandas as pd

# Global
OUTPUT_XLSX = "snow_incidents.xlsx"
WORKSHEET_NAME = "Incidents"


def pull_servicenow_data():
    # Set the request parameters
    url = 'https://dev181336.service-now.com/api/now/table/x_1854014_incide_0_incidents?sysparm_display_value=all'

    # Eg. User name="admin", Password="admin" for this code sample.
    user = 'admin'
    pwd = 'z$2V8r6R7CY_,Lc]Dj6o}nh%AEV^WXW3_c7BRn=.&P^X{HXrfU174X2n:k!MmfE#e-h'

    # Set proper headers
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    # Do the HTTP request
    response = requests.get(url, auth=(user, pwd), headers=headers)

    # Check for HTTP codes other than 200
    if response.status_code != 200:
        print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:', response.json())
        exit()

    # Decode the JSON response into a dictionary and use the data
    data = response.json()
    return data


def write_servicenow_data():
    records_dict = pull_servicenow_data()

    # Access the list of incidents using the 'result' key
    incidents_list = records_dict.get('result', [])

    if not incidents_list:
        print("No records returned or 'result' key missing in JSON response.")
        return

    # Convert list of dicts to DataFrame
    # Use incidents_list instead of records_dict
    df = pd.json_normalize(incidents_list)

    # Rename columns for readability (optional)
    rename_map = {
        "number": "Incident Number",
        "sys_created_on": "Reported Date",
        "caller_id.display_value": "Client Name",
        "short_description": "Short Description",
        "description": "Detailed Description",
        "category": "Category",
        "subcategory": "Subcategory",
        "cmdb_ci.display_value": "Configuration Item",
        "impact": "Impact",
        "urgency": "Urgency",
        "priority": "Priority",
        "state": "Status",
        "assignment_group.display_value": "Assigned Group",
        "assigned_to.display_value": "Assigned To",
        "resolved_at": "Resolved Date/Time",
        "closed_at": "Closed Date/Time",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Write to Excel
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=WORKSHEET_NAME, index=False)

    print(f"Wrote Excel: {OUTPUT_XLSX} / {WORKSHEET_NAME} ({len(df)} rows) and columns: {list(df.columns)}")


write_servicenow_data()


