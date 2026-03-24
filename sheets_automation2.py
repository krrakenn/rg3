import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
from calendar import monthrange
import json
import os
import re
import streamlit as st
from zoneinfo import ZoneInfo
from utils import get_secret
from urllib.parse import parse_qs, urlparse
from sql_generator import rewrite_sql_date_window_llm

AUTOMATION_SHEET_URL = "https://docs.google.com/spreadsheets/d/1pmHIwxTZA2fwfewUBAtW7-UE4Nq3YU1r2DEw5qaQ-XM/edit?gid=0#gid=0"
AUTOMATION_WORKSHEET_TITLE = "Automations"
IST = ZoneInfo("Asia/Kolkata")
AUTOMATION_HEADERS = [
    "id",
    "sheet_url",
    "sql_query",
    "refresh_frequency",
    "layout_mapping",
    "query_type",
    "last_run",
    "created_at",
    "last_updated",
    "window_start",
    "window_end"
]
DATE_LITERAL_PATTERN = re.compile(r"(?<!\d)(\d{4})[-/](\d{2})[-/](\d{2})(?!\d)")
EXCLUSIVE_END_PATTERN_TEMPLATE = r"<\s*(?:timestamp\s*)?['\"]?{window_end}(?:\s+00:00:00)?['\"]?"
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]


def get_current_ist_datetime():
    return datetime.now(IST)


def format_sheet_timestamp(value=None):
    timestamp = value or get_current_ist_datetime()
    return timestamp.isoformat(timespec="seconds")


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
        header_end_cell = gspread.utils.rowcol_to_a1(1, len(AUTOMATION_HEADERS))
        worksheet.update(f"A1:{header_end_cell}", [AUTOMATION_HEADERS])
        worksheet.format(f"A1:{header_end_cell}", {"textFormat": {"bold": True}})

    return worksheet


def _extract_gid_from_sheet_url(sheet_url):
    parsed_url = urlparse(sheet_url)

    for raw_part in (parsed_url.query, parsed_url.fragment):
        if not raw_part:
            continue

        params = parse_qs(raw_part)
        gid_values = params.get("gid")
        if gid_values:
            gid_value = gid_values[0].strip()
            if gid_value.isdigit():
                return int(gid_value)

    return None


def _get_target_worksheet(spreadsheet, sheet_url):
    gid = _extract_gid_from_sheet_url(sheet_url)

    if gid is not None:
        for worksheet in spreadsheet.worksheets():
            if worksheet.id == gid:
                return worksheet

        raise ValueError(f"No worksheet found for gid={gid} in the provided Google Sheet URL")

    try:
        return spreadsheet.sheet1
    except Exception:
        return spreadsheet.add_worksheet(title="Report", rows=1000, cols=200)


def init_db():
    return get_automation_worksheet()


def _parse_iso_date(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value.date()
    return datetime.fromisoformat(str(value)).date()


def _format_iso_date(value):
    parsed_date = _parse_iso_date(value)
    return parsed_date.isoformat() if parsed_date else None


def _is_exclusive_window_end(sql_query, window_end):
    if not sql_query or not window_end:
        return False

    normalized_end = _format_iso_date(window_end)
    if not normalized_end:
        return False

    pattern = EXCLUSIVE_END_PATTERN_TEMPLATE.format(window_end=re.escape(normalized_end))
    return re.search(pattern, sql_query, flags=re.IGNORECASE) is not None


def _get_display_window_end(sql_query, window_start, window_end):
    start_date = _parse_iso_date(window_start)
    end_date = _parse_iso_date(window_end)

    if not start_date or not end_date:
        return _format_iso_date(window_end)

    if end_date > start_date and _is_exclusive_window_end(sql_query, window_end):
        return (end_date - timedelta(days=1)).isoformat()

    return end_date.isoformat()


def infer_query_window(sql_query, query_type="no_date"):
    if query_type != "with_date" or not sql_query:
        return None, None

    found_dates = []
    seen_dates = set()

    for match in DATE_LITERAL_PATTERN.finditer(sql_query):
        normalized = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
        if normalized not in seen_dates:
            found_dates.append(normalized)
            seen_dates.add(normalized)

    if len(found_dates) >= 2:
        return found_dates[0], found_dates[1]

    if len(found_dates) == 1:
        return found_dates[0], found_dates[0]

    return None, None


def _add_months(value, months):
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def shift_query_window(window_start, window_end, frequency):
    start_date = _parse_iso_date(window_start)
    end_date = _parse_iso_date(window_end)

    if not start_date or not end_date:
        return None, None

    frequency_key = (frequency or "").lower()

    if frequency_key == "daily":
        delta = timedelta(days=1)
        next_start = start_date + delta
        next_end = end_date + delta
    elif frequency_key == "weekly":
        delta = timedelta(days=7)
        next_start = start_date + delta
        next_end = end_date + delta
    elif frequency_key == "monthly":
        next_start = _add_months(start_date, 1)
        next_end = _add_months(end_date, 1)
    else:
        return None, None

    return next_start.isoformat(), next_end.isoformat()


def rewrite_query_window(sql_query, new_start, new_end):
    if not sql_query:
        return sql_query

    replacements = [new_start, new_end]
    replacement_index = 0
    seen_dates = set()

    def replace_match(match):
        nonlocal replacement_index
        normalized = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

        if normalized in seen_dates or replacement_index >= len(replacements):
            return match.group(0)

        seen_dates.add(normalized)
        replacement_value = replacements[replacement_index]
        replacement_index += 1
        separator = "-" if "-" in match.group(0) else "/"
        return replacement_value.replace("-", separator)

    return DATE_LITERAL_PATTERN.sub(replace_match, sql_query)


def rewrite_query_window_with_llm(sql_query, current_start, current_end, new_start, new_end):
    if not sql_query:
        return sql_query

    if current_start and current_end:
        try:
            return rewrite_sql_date_window_llm(
                sql_query,
                current_start,
                current_end,
                new_start,
                new_end
            )
        except Exception:
            pass

    return rewrite_query_window(sql_query, new_start, new_end)


def _get_automation_column_map(worksheet):
    return {
        header: index
        for index, header in enumerate(worksheet.row_values(1), start=1)
        if header
    }


def store_automation(sheet_url, sql_query, refresh_frequency, layout_mapping, query_type="no_date"):
    worksheet = get_automation_worksheet()
    existing_values = worksheet.col_values(1)[1:]
    existing_ids = [int(value) for value in existing_values if str(value).strip().isdigit()]
    automation_id = max(existing_ids, default=0) + 1
    now = format_sheet_timestamp()
    window_start, window_end = infer_query_window(sql_query, query_type)

    worksheet.append_row([
        automation_id,
        sheet_url,
        sql_query,
        refresh_frequency,
        json.dumps(layout_mapping),
        query_type,
        now,
        now,
        now,
        window_start or "",
        window_end or ""
    ], value_input_option="RAW")

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
    now = format_sheet_timestamp()
    column_map = _get_automation_column_map(worksheet)
    last_run_col = column_map["last_run"]
    last_updated_col = column_map["last_updated"]
    last_run_cell = gspread.utils.rowcol_to_a1(row_number, last_run_col)
    last_updated_cell = gspread.utils.rowcol_to_a1(row_number, last_updated_col)
    worksheet.batch_update([
        {"range": last_run_cell, "values": [[now]]},
        {"range": last_updated_cell, "values": [[now]]}
    ], value_input_option="RAW")


def update_automation_execution_state(row_number, sql_query=None, window_start=None, window_end=None, last_run=None):
    worksheet = get_automation_worksheet()
    column_map = _get_automation_column_map(worksheet)
    now = format_sheet_timestamp()
    batch_updates = []

    if sql_query is not None:
        batch_updates.append({
            "range": gspread.utils.rowcol_to_a1(row_number, column_map["sql_query"]),
            "values": [[sql_query]]
        })

    if "window_start" in column_map and window_start is not None:
        batch_updates.append({
            "range": gspread.utils.rowcol_to_a1(row_number, column_map["window_start"]),
            "values": [[window_start]]
        })

    if "window_end" in column_map and window_end is not None:
        batch_updates.append({
            "range": gspread.utils.rowcol_to_a1(row_number, column_map["window_end"]),
            "values": [[window_end]]
        })

    if last_run is not None:
        batch_updates.append({
            "range": gspread.utils.rowcol_to_a1(row_number, column_map["last_run"]),
            "values": [[last_run]]
        })

    batch_updates.append({
        "range": gspread.utils.rowcol_to_a1(row_number, column_map["last_updated"]),
        "values": [[now]]
    })

    worksheet.batch_update(batch_updates, value_input_option="RAW")


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


def generate_column_header(query_type, frequency, window_start=None, window_end=None, sql_query=None):
    if query_type == "with_date" and window_start and window_end:
        start_value = _format_iso_date(window_start)
        end_value = _get_display_window_end(sql_query, window_start, window_end)
        if start_value == end_value:
            return start_value
        return f"{start_value} - {end_value}"

    today = get_current_ist_datetime()
    
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


def _normalize_sheet_value(value):
    if pd.isna(value):
        return ""
    return value


def _build_column_range_values(existing_column_values, total_metrics):
    column_values = []

    for row in range(2, total_metrics + 2):
        current_value = ""
        if len(existing_column_values) >= row:
            current_value = existing_column_values[row - 1]
        column_values.append([current_value])

    return column_values

def write_report_to_sheet(sheet_url, result_df, refresh_frequency, query_type="no_date", execution_window_start=None, execution_window_end=None, sql_query=None):
    layout_mapping = generate_layout_mapping(result_df)
    client = get_gspread_client()
    sheet = client.open_by_url(sheet_url)
    ws = _get_target_worksheet(sheet, sheet_url)

    column_header = generate_column_header(
        query_type,
        refresh_frequency,
        window_start=execution_window_start,
        window_end=execution_window_end,
        sql_query=sql_query
    )

    batch_updates = []

    if ws.cell(1, 1).value is None:
        batch_updates.append({
            "range": "A1",
            "values": [["KPIs"]]
        })

    existing_dates = get_existing_dates(ws)
    if column_header in existing_dates:
        date_col = existing_dates[column_header]
    else:
        date_col = len(existing_dates) + 2
        batch_updates.append({
            "range": gspread.utils.rowcol_to_a1(1, date_col),
            "values": [[column_header]]
        })

    existing_metrics = get_existing_metrics(ws)
    new_metrics = []

    for metric in layout_mapping:
        if metric not in existing_metrics:
            row = len(existing_metrics) + 2
            existing_metrics[metric] = row
            new_metrics.append(metric)

    if new_metrics:
        start_row = existing_metrics[new_metrics[0]]
        end_row = existing_metrics[new_metrics[-1]]
        metric_values = [[metric] for metric in new_metrics]
        batch_updates.append({
            "range": f"A{start_row}:A{end_row}",
            "values": metric_values
        })

    total_metrics = len(existing_metrics)

    existing_column_values = []
    if total_metrics > 0:
        existing_column_values = ws.col_values(date_col)

    column_values = _build_column_range_values(existing_column_values, total_metrics)

    for metric, value in layout_mapping.items():
        row = existing_metrics[metric]
        column_values[row - 2] = [_normalize_sheet_value(value)]

    if column_values:
        start_cell = gspread.utils.rowcol_to_a1(2, date_col)
        end_cell = gspread.utils.rowcol_to_a1(total_metrics + 1, date_col)
        batch_updates.append({
            "range": f"{start_cell}:{end_cell}",
            "values": column_values
        })

    if batch_updates:
        ws.batch_update(batch_updates, value_input_option="USER_ENTERED")

    ws.format("A1", {"textFormat": {"bold": True}})
    ws.format(gspread.utils.rowcol_to_a1(1, date_col), {"textFormat": {"bold": True}})
    ws.format(f"A1:A{len(existing_metrics)+1}", {"textFormat": {"bold": True}})
    
    return {
        "sheet_url": sheet_url,
        "refresh_frequency": refresh_frequency,
        "query_type": query_type,
        "column_header": column_header,
        "status": "success"
    }


def automate_report(sheet_url, result_df, sql_query, refresh_frequency, query_type="no_date", register_automation=True):
    execution_window_start, execution_window_end = infer_query_window(sql_query, query_type)
    init_db()
    response = write_report_to_sheet(
        sheet_url=sheet_url,
        result_df=result_df,
        refresh_frequency=refresh_frequency,
        query_type=query_type,
        execution_window_start=execution_window_start,
        execution_window_end=execution_window_end,
        sql_query=sql_query
    )

    if execution_window_start:
        response["window_start"] = execution_window_start
    if execution_window_end:
        response["window_end"] = execution_window_end

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
