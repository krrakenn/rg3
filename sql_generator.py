import os
import json
from openai import OpenAI
from dotenv import load_dotenv
import streamlit as st
from utils import get_secret
load_dotenv()


def _get_llm_client():
    return OpenAI(
        api_key=get_secret("LLM_API_KEY") or os.getenv("LLM_API_KEY"),
        base_url="https://imllm.intermesh.net/v1"
    )


def _strip_markdown_fences(value):
    cleaned = value.strip()
    cleaned = cleaned.replace("```json", "").replace("```sql", "").replace("```", "")
    return cleaned.strip()


def _extract_json_object(value):
    cleaned = _strip_markdown_fences(value)
    start_index = cleaned.find("{")
    end_index = cleaned.rfind("}")

    if start_index == -1 or end_index == -1 or end_index < start_index:
        raise ValueError("No JSON object found in LLM response")

    return json.loads(cleaned[start_index:end_index + 1])

def merge_queries_llm(queries):
    client = _get_llm_client()

    queries_text = "\n\n".join(queries)

    prompt = f"""
You are an expert SQL developer.

The user provided multiple SQL queries.

If they can be merged into ONE optimized SQL query, merge them.

If they cannot be merged, return them as separate SELECT statements.

RULES
- Only SELECT queries
- No INSERT, DELETE, UPDATE
- Use schema im_dwh_rpt only

Queries:
{queries_text}

Return ONLY SQL.
"""

    completion = client.chat.completions.create(
        model="openai/gpt-5.2",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    sql = completion.choices[0].message.content.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()

    return sql


def generate_sql(schema_context, kpis, additional_prompt):
    client = _get_llm_client()

    prompt = f"""
You are an expert analytics engineer and SQL developer.

Your task is to generate ONE SQL query that calculates ALL the requested KPIs.

You must strictly follow the rules below.

------------------------
CRITICAL RULES
------------------------

1. ONLY use tables from the schema: im_dwh_rpt
2. ALWAYS reference tables using the fully qualified format:
   im_dwh_rpt.table_name

3. ONLY use columns that exist in the provided schema context.
   - DO NOT invent columns
   - DO NOT assume columns
   - DO NOT use external knowledge

4. Carefully read:
   - The schema context
   - The KPI definitions
   - The additional instructions

5. When selecting columns:
   - Choose the column that BEST matches the KPI definition
   - Prefer columns explicitly referenced in the additional instructions
   - Use the MINIMUM number of columns necessary

6. If multiple tables contain similar columns:
   - Select the table whose structure most closely matches the KPI definition

7. Return ALL KPIs in ONE result ROW.

8. Each KPI must be returned as a column with the exact KPI name as the alias.

Example format:

SELECT
    <metric_calculation> AS kpi_1,
    <metric_calculation> AS kpi_2,
    <metric_calculation> AS kpi_3
FROM ...

9. Use JOINs only when required.

10. Do NOT include:
    - explanations
    - comments
    - markdown
    - extra text

Return ONLY the SQL query.

------------------------
REASONING PROCESS (INTERNAL)
------------------------

Before writing the SQL query:

1. Identify which schema tables contain relevant data.
2. Identify the columns required to compute each KPI.
3. Verify that those columns exist in the schema context.
4. Determine necessary joins between tables.
5. Construct the SQL query that calculates all KPIs in one result.

Do NOT output this reasoning.

------------------------
SCHEMA CONTEXT
------------------------
{schema_context}

------------------------
KPI DEFINITIONS
------------------------
{kpis}

------------------------
ADDITIONAL INFORMATION
------------------------
{additional_prompt}

------------------------
FINAL OUTPUT
------------------------

Return ONLY ONE SQL query.
"""

    completion = client.chat.completions.create(
        model="openai/gpt-5.2",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    sql = completion.choices[0].message.content.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()
    return sql


def generate_sql_chat_response(schema_context, selected_tables, chat_history, current_sql=None):
    client = _get_llm_client()

    formatted_history = []
    for message in chat_history:
        role = (message.get("role") or "user").upper()
        content = (message.get("content") or "").strip()
        if content:
            formatted_history.append(f"{role}: {content}")

    history_text = "\n\n".join(formatted_history) or "No conversation yet."
    selected_tables_text = ", ".join(selected_tables)

    prompt = f"""
You are an expert analytics engineer and SQL developer helping a user build a report conversationally.

Your task is to read the full chat history, understand the latest user request, and return:
1. a short assistant reply
2. the latest SQL draft

Rules:
- Use only schema im_dwh_rpt.
- Use only tables and columns supported by the provided schema context.
- Keep the assistant reply short and practical.
- If the user is refining an existing report, update the current SQL draft instead of starting over unless the request clearly changes the report.
- Return only valid JSON.
- Do not wrap the response in markdown.

Return this exact JSON shape:
{{
  "assistant_message": "short reply",
  "sql": "full SQL query"
}}

Selected tables: {selected_tables_text}

Schema context:
{schema_context}

Current SQL draft:
{current_sql or "None"}

Chat history:
{history_text}
"""

    completion = client.chat.completions.create(
        model="openai/gpt-5.2",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    payload = completion.choices[0].message.content.strip()
    response = _extract_json_object(payload)
    response["assistant_message"] = (response.get("assistant_message") or "Updated the SQL draft.").strip()
    response["sql"] = _strip_markdown_fences(response.get("sql") or "")
    return response


def rewrite_sql_date_window_llm(sql_query, current_start, current_end, new_start, new_end):
    client = _get_llm_client()

    prompt = f"""
You are an expert SQL editor.

Update the SQL query so that the actual covered reporting window changes from the current inclusive window to the new inclusive window.

Rules:
- Keep the query logic, joins, aliases, filters, formatting, and casing unchanged unless a date literal must change.
- Only replace the date values that define the reporting window.
- Preserve the SQL's date-filter style where appropriate. For example, if the query currently uses an exclusive upper bound like `< TIMESTAMP '2026-03-22 00:00:00'`, keep using an exclusive upper bound for the rewritten query.
- Do not add comments, explanations, or markdown.
- Return only SQL.

Current inclusive window start: {current_start}
Current inclusive window end: {current_end}
New inclusive window start: {new_start}
New inclusive window end: {new_end}

SQL:
{sql_query}
"""

    completion = client.chat.completions.create(
        model="openai/gpt-5.2",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    sql = completion.choices[0].message.content.strip()
    return sql.replace("```sql", "").replace("```", "").strip()


def analyze_sql_date_window_llm(sql_query, frequency=None):
    client = _get_llm_client()

    prompt = f"""
You are analyzing a SQL query to determine the ACTUAL reporting window covered by its date filters.

Rules:
- Infer the true covered date range from the SQL itself.
- The returned window_start and window_end must both be inclusive dates in YYYY-MM-DD format.
- If the query uses an exclusive upper bound like `< TIMESTAMP '2026-03-22 00:00:00'`, then the inclusive end date is 2026-03-21.
- If the query uses `<=` on an end date, that end date remains inclusive.
- Prefer the main reporting window used by the query.
- Return ONLY valid JSON.

Return this exact JSON shape:
{{
  "window_start": "YYYY-MM-DD or null",
  "window_end": "YYYY-MM-DD or null",
  "header": "short spreadsheet header or null"
}}

Header rules:
- Weekly example: `15-21 Mar 26`
- Daily example: `15 Mar 26`
- Monthly example: `Mar 26`
- Use the actual inclusive covered dates, not raw SQL bounds.

Frequency: {frequency or "unknown"}

SQL:
{sql_query}
"""

    completion = client.chat.completions.create(
        model="openai/gpt-5.2",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    payload = completion.choices[0].message.content.strip()
    return _extract_json_object(payload)


def analyze_sheet_header_window_llm(header_text, frequency=None):
    client = _get_llm_client()

    prompt = f"""
You are analyzing a spreadsheet column header that represents a reporting period.

Rules:
- Infer the inclusive reporting window represented by the header text.
- Return ONLY valid JSON.
- window_start and window_end must be inclusive dates in YYYY-MM-DD format.
- If the header is ambiguous, use the provided frequency to infer the most reasonable window.
- If the header does not contain a usable reporting period, return null values.

Return this exact JSON shape:
{{
  "window_start": "YYYY-MM-DD or null",
  "window_end": "YYYY-MM-DD or null"
}}

Frequency: {frequency or "unknown"}
Header:
{header_text}
"""

    completion = client.chat.completions.create(
        model="openai/gpt-5.2",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    payload = completion.choices[0].message.content.strip()
    return _extract_json_object(payload)


def generate_column_header_llm(sql_query, frequency, window_start, window_end):
    analysis = analyze_sql_date_window_llm(sql_query, frequency=frequency)
    return (analysis.get("header") or "").strip()
