import logging
from datetime import datetime, timedelta
from query_runner import run_sql
from sheets_automation2 import (
    automate_report,
    init_db,
    infer_query_window,
    list_automations,
    rewrite_query_window,
    shift_query_window,
    update_automation_execution_state,
    update_automation_last_run,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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

    updated_query = rewrite_query_window(sql_query, next_window_start, next_window_end)
    return updated_query, next_window_start, next_window_end


def get_due_automations():
    now = datetime.now()
    due_automations = []

    automations = list_automations()

    for auto in automations:
        auto_id = auto["id"]
        sheet_url = auto["sheet_url"]
        sql_query = auto["sql_query"]
        frequency = auto["refresh_frequency"]
        query_type = auto.get("query_type") or "no_date"
        last_run = auto.get("last_run")
        
        if not last_run:
            is_due = True
        else:
            last_run_dt = datetime.fromisoformat(last_run)
            
            if frequency.lower() == "daily":
                is_due = (now - last_run_dt).days >= 1
            elif frequency.lower() == "weekly":
                is_due = (now - last_run_dt).days >= 7
            elif frequency.lower() == "monthly":
                is_due = (now - last_run_dt).days >= 30
            else:
                is_due = False
        
        if is_due:
            due_automations.append({
                "id": auto_id,
                "row_number": auto["row_number"],
                "sheet_url": sheet_url,
                "sql_query": sql_query,
                "frequency": frequency,
                "query_type": query_type,
                "window_start": auto.get("window_start") or None,
                "window_end": auto.get("window_end") or None
            })
    
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
                last_run=datetime.now().isoformat(timespec="seconds")
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
        for automation in due_automations:
            run_automation(automation)

    except Exception as e:
        logger.error(f"Scheduler error: {str(e)}")


if __name__ == "__main__":
    run_scheduler_once()
