import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
import json
import os
import streamlit as st
from utils import get_secret
import time

AUTOMATION_SHEET_URL = "https://docs.google.com/spreadsheets/d/1pmHIwxTZA2fwfewUBAtW7-UE4Nq3YU1r2DEw5qaQ-XM/edit?gid=0#gid=0"
AUTOMATION_WORKSHEET_TITLE = "Automations"
AUTOMATION_HEADERS = [
    "id",
    "sheet_url",
    "sql_query",
    "refresh_frequency",
    "layout_mapping",
    "query_type",
    "last_run",
    "created_at",
    "last_updated"
]
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]


def _get_service_account_info():
    service_account_info = get_secret("SERVICE_ACCOUNT_JSON")

    if service_account_info:
        if isinstance(service_account_info, str):
            return json.loads(service_account_info)
        return service_account_info

    gcp_service_account = None

    try:
        if "gcp_service_account" in st.secrets:
            gcp_service_account = dict(st.secrets["gcp_service_account"])
    except Exception:
        gcp_service_account = None

    if gcp_service_account:
        return gcp_service_account

    env_value = os.getenv("SERVICE_ACCOUNT_JSON")
    if env_value:
        return json.loads(env_value)

    raise ValueError(
        "Missing Google Sheets credentials. Add SERVICE_ACCOUNT_JSON or [gcp_service_account] in Streamlit secrets."
    )


def get_gspread_client():
    credentials_info = _get_service_account_info()
    creds = Credentials.from_service_account_info(
        credentials_info,
        scopes=scopes
    )
    return gspread.authorize(creds)


def get_automation_worksheet():
    if not AUTOMATION_SHEET_URL:
        raise ValueError("Missing AUTOMATION_SHEET_URL secret")

    client = get_gspread_client()
    spreadsheet = client.open_by_url(AUTOMATION_SHEET_URL)

    try:
        worksheet = spreadsheet.worksheet(AUTOMATION_WORKSHEET_TITLE)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=AUTOMATION_WORKSHEET_TITLE,
            rows=1000,
            cols=len(AUTOMATION_HEADERS) + 2
        )

    if worksheet.row_values(1) != AUTOMATION_HEADERS:
        worksheet.update("A1:I1", [AUTOMATION_HEADERS])
        worksheet.format("A1:I1", {"textFormat": {"bold": True}})

    return worksheet


def init_db():
    return get_automation_worksheet()


def store_automation(sheet_url, sql_query, refresh_frequency, layout_mapping, query_type="no_date"):
    worksheet = get_automation_worksheet()
    existing_values = worksheet.col_values(1)[1:]
    existing_ids = [int(value) for value in existing_values if str(value).strip().isdigit()]
    automation_id = max(existing_ids, default=0) + 1
    now = datetime.now().isoformat(timespec="seconds")

    worksheet.append_row([
        automation_id,
        sheet_url,
        sql_query,
        refresh_frequency,
        json.dumps(layout_mapping),
        query_type,
        "",
        now,
        now
    ], value_input_option="USER_ENTERED")

    return automation_id


def list_automations():
    worksheet = get_automation_worksheet()
    records = worksheet.get_all_records()
    automations = []

    for index, record in enumerate(records, start=2):
        record["row_number"] = index
        automations.append(record)

    return automations


def update_automation_last_run(row_number):
    worksheet = get_automation_worksheet()
    now = datetime.now().isoformat(timespec="seconds")
    worksheet.update(f"G{row_number}:I{row_number}", [[now, worksheet.cell(row_number, 8).value, now]])


def generate_layout_mapping(df):
    mapping = {}
    df = df.copy()

    if df.shape[1] > 1 and pd.api.types.is_string_dtype(df.iloc[:, 0]):
        entity_col = df.columns[0]
        for _, row in df.iterrows():
            entity = row[entity_col]
            for metric in df.columns[1:]:
                key = f"{entity} - {metric}"
                mapping[key] = row[metric]
        return mapping

    if len(df) == 1:
        row = df.iloc[0]
        for metric, value in row.items():
            mapping[metric] = value
        return mapping

    for idx, row in df.iterrows():
        for metric, value in row.items():
            key = f"{idx} - {metric}"
            mapping[key] = value

    return mapping


def get_existing_metrics(ws):
    col = ws.col_values(1)
    metric_rows = {}

    for i, val in enumerate(col):
        if i == 0:
            continue
        metric_rows[val] = i + 1

    return metric_rows


def get_existing_dates(ws):
    row = ws.row_values(1)
    date_cols = {}

    for i, val in enumerate(row):
        if i == 0:
            continue
        date_cols[val] = i + 1

    return date_cols


def generate_column_header(query_type, frequency):
    today = datetime.now()
    
    if query_type == "no_date":
        if frequency.lower() == "daily":
            return today.strftime("%Y-%m-%d")
        
        elif frequency.lower() == "weekly":
            week_num = today.isocalendar()[1]
            month_name = today.strftime("%b")
            return f"{month_name} Week {week_num}"
        
        elif frequency.lower() == "monthly":
            return today.strftime("%B")
    
    elif query_type == "with_date":
        if frequency.lower() == "daily":
            return today.strftime("%Y-%m-%d")
        
        elif frequency.lower() == "weekly":
            start_date = (today - timedelta(days=7)).strftime("%d")
            end_date = today.strftime("%d")
            month_name = today.strftime("%b")
            return f"{month_name} {start_date}-{end_date}"
        
        elif frequency.lower() == "monthly":
            return today.strftime("%B")
    
    return today.strftime("%Y-%m-%d")

def write_report_to_sheet(sheet_url, result_df, refresh_frequency, query_type="no_date"):
    layout_mapping = generate_layout_mapping(result_df)
    client = get_gspread_client()
    sheet = client.open_by_url(sheet_url)

    try:
        ws = sheet.sheet1
    except Exception:
        ws = sheet.add_worksheet(title="Report", rows=1000, cols=200)

    column_header = generate_column_header(query_type, refresh_frequency)

    if ws.cell(1, 1).value is None:
        ws.update_cell(1, 1, "KPIs")
        ws.format("A1", {"textFormat": {"bold": True}})

    existing_dates = get_existing_dates(ws)
    if column_header in existing_dates:
        date_col = existing_dates[column_header]
    else:
        date_col = len(existing_dates) + 2
        ws.update_cell(1, date_col, column_header)
        ws.format(gspread.utils.rowcol_to_a1(1, date_col), {"textFormat": {"bold": True}})

    existing_metrics = get_existing_metrics(ws)

    for metric, value in layout_mapping.items():
        if metric in existing_metrics:
            row = existing_metrics[metric]
        else:
            row = len(existing_metrics) + 2
            ws.update_cell(row, 1, metric)
            time.sleep(0.1)
            existing_metrics[metric] = row
        ws.update_cell(row, date_col, value)
        time.sleep(0.1)
    
    ws.format(f"A1:A{len(existing_metrics)+1}", {"textFormat": {"bold": True}})
    
    return {
        "sheet_url": sheet_url,
        "refresh_frequency": refresh_frequency,
        "query_type": query_type,
        "status": "success"
    }


def automate_report(sheet_url, result_df, sql_query, refresh_frequency, query_type="no_date", register_automation=True):
    init_db()
    response = write_report_to_sheet(
        sheet_url=sheet_url,
        result_df=result_df,
        refresh_frequency=refresh_frequency,
        query_type=query_type
    )

    if register_automation:
        layout_mapping = generate_layout_mapping(result_df)
        response["automation_id"] = store_automation(
            sheet_url,
            sql_query,
            refresh_frequency,
            layout_mapping,
            query_type
        )

    return response
