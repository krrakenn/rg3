import logging
from calendar import monthrange
from datetime import datetime, timedelta
from query_runner import run_sql
from sheets_automation2 import (
    IST,
    automate_report,
    format_sheet_timestamp,
    get_current_ist_datetime,
    init_db,
    infer_query_window,
    list_automations,
    rewrite_query_window_with_llm,
    shift_query_window,
    update_automation_execution_state,
    update_automation_last_run,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _ensure_ist_datetime(value):
    if value.tzinfo is None:
        return value.replace(tzinfo=IST)
    return value.astimezone(IST)


def parse_datetime_safe(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return _ensure_ist_datetime(value)
    normalized = str(value).strip()
    try:
        return _ensure_ist_datetime(datetime.fromisoformat(normalized))
    except ValueError:
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%d/%m/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
            "%d/%m/%Y",
            "%m/%d/%Y",
        ]

        for fmt in formats:
            try:
                return _ensure_ist_datetime(datetime.strptime(normalized, fmt))
            except ValueError:
                continue

        logger.error(f"Invalid datetime format: {value}")
        return None


def _add_months(value, months):
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _parse_schedule_date(value):
    parsed_datetime = parse_datetime_safe(value)
    return parsed_datetime.date() if parsed_datetime else None


def _get_latest_scheduled_date(schedule_start_date, frequency, current_date):
    if not schedule_start_date or current_date < schedule_start_date:
        return None

    frequency_key = (frequency or "").lower()

    if frequency_key == "daily":
        elapsed_days = (current_date - schedule_start_date).days
        return schedule_start_date + timedelta(days=elapsed_days)

    if frequency_key == "weekly":
        elapsed_days = (current_date - schedule_start_date).days
        elapsed_weeks = elapsed_days // 7
        return schedule_start_date + timedelta(days=elapsed_weeks * 7)

    if frequency_key == "monthly":
        latest_date = schedule_start_date
        while True:
            next_date = _add_months(latest_date, 1)
            if next_date > current_date:
                return latest_date
            latest_date = next_date

    return None


def _next_due_datetime(last_run_dt, frequency):
    frequency_key = (frequency or "").lower()

    if frequency_key == "daily":
        return last_run_dt + timedelta(days=1)

    if frequency_key == "weekly":
        return last_run_dt + timedelta(days=7)

    if frequency_key == "monthly":
        return _add_months(last_run_dt, 1)

    return None


def _is_automation_due(now, frequency, last_run=None, schedule_start_date=None):
    start_date = _parse_schedule_date(schedule_start_date)
    last_run_dt = parse_datetime_safe(last_run) if last_run else None

    if not last_run_dt:
        if start_date and now.date() < start_date:
            return False
        return True

    next_due_at = _next_due_datetime(last_run_dt, frequency)
    if not next_due_at:
        return False

    if start_date and last_run_dt.date() < start_date and now.date() < start_date:
        return False

    return now >= next_due_at


def resolve_scheduled_query(sql_query, frequency, query_type, window_start=None, window_end=None):
    if query_type != "with_date":
        return sql_query, None, None

    inferred_start, inferred_end = infer_query_window(sql_query, query_type)
    current_window_start = inferred_start or window_start
    current_window_end = inferred_end or window_end

    if not current_window_start or not current_window_end:
        return sql_query, None, None

    next_window_start, next_window_end = shift_query_window(
        current_window_start,
        current_window_end,
        frequency
    )

    if not next_window_start or not next_window_end:
        return sql_query, None, None

    updated_query = rewrite_query_window_with_llm(
        sql_query,
        current_window_start,
        current_window_end,
        next_window_start,
        next_window_end
    )
    return updated_query, next_window_start, next_window_end


def get_due_automations():
    now = get_current_ist_datetime()
    due_automations = []

    automations = list_automations()

    for auto in automations:
        try:
            auto_id = auto["id"]
            sheet_url = auto["sheet_url"]
            sql_query = auto["sql_query"]
            frequency = auto["refresh_frequency"]
            query_type = auto.get("query_type") or "no_date"
            last_run = auto.get("last_run")
            schedule_start_date = auto.get("schedule_start_date") or None

            is_due = _is_automation_due(
                now,
                frequency,
                last_run=last_run,
                schedule_start_date=schedule_start_date
            )

            if is_due:
                due_automations.append({
                    "id": auto_id,
                    "row_number": auto["row_number"],
                    "sheet_url": sheet_url,
                    "sql_query": sql_query,
                    "frequency": frequency,
                    "query_type": query_type,
                    "window_start": auto.get("window_start") or None,
                    "window_end": auto.get("window_end") or None,
                    "schedule_start_date": schedule_start_date
                })

        except Exception as e:
            logger.error(f"Skipping automation due to error: {str(e)}")

    return due_automations


def run_automation(automation):
    auto_id = automation["id"]
    row_number = automation["row_number"]
    sheet_url = automation["sheet_url"]
    sql_query = automation["sql_query"]
    frequency = automation["frequency"]
    query_type = automation["query_type"]
    window_start = automation.get("window_start")
    window_end = automation.get("window_end")

    try:
        logger.info(f"Running automation {auto_id}")

        final_query, next_window_start, next_window_end = resolve_scheduled_query(
            sql_query,
            frequency,
            query_type,
            window_start=window_start,
            window_end=window_end
        )

        if next_window_start and next_window_end:
            logger.info(f"Using shifted date window {next_window_start} to {next_window_end}")

        logger.info(f"Executing query...")
        result_df = run_sql(final_query)

        logger.info(f"Pushing to Google Sheet: {sheet_url}")
        automate_report(
            sheet_url=sheet_url,
            result_df=result_df,
            sql_query=final_query,
            refresh_frequency=frequency,
            query_type=query_type,
            register_automation=False
        )

        if next_window_start and next_window_end:
            update_automation_execution_state(
                row_number,
                sql_query=final_query,
                window_start=next_window_start,
                window_end=next_window_end,
                last_run=format_sheet_timestamp()
            )
        else:
            update_automation_last_run(row_number)

        logger.info(f"Automation {auto_id} completed successfully")
        return {"status": "success", "auto_id": auto_id}

    except Exception as e:
        logger.error(f"Automation {auto_id} failed: {str(e)}")
        return {"status": "failed", "auto_id": auto_id, "error": str(e)}


def run_scheduler_once():
    init_db()
    logger.info("Running scheduler cycle")
    try:
        due_automations = get_due_automations()
        logger.info(f"Due automations count: {len(due_automations)}")

        for automation in due_automations:
            run_automation(automation)

    except Exception as e:
        logger.error(f"Scheduler error: {str(e)}")


if __name__ == "__main__":
    run_scheduler_once()
