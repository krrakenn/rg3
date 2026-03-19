import os
from openai import OpenAI
from dotenv import load_dotenv
import streamlit as st
from utils import get_secret
load_dotenv()

def merge_queries_llm(queries):

    client = OpenAI(
        api_key=get_secret("LLM_API_KEY"),
        base_url="https://imllm.intermesh.net/v1"
    )

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

    client = OpenAI(
        api_key=os.getenv("LLM_API_KEY"),
        base_url="https://imllm.intermesh.net/v1"
    )

    prompt = f"""
You are a senior analytics engineer writing production-safe SQL for a data warehouse.

========================
OBJECTIVE
========================

Generate exactly ONE SQL query that computes ALL requested KPIs using ONLY the schema provided.
The query must be correct, minimal, and safe to run on a production database.

========================
NON-NEGOTIABLE RULES
========================

SCHEMA DISCIPLINE
- Use ONLY tables from schema: im_dwh_rpt
- Fully qualify every table: im_dwh_rpt.table_name
- Use ONLY columns explicitly listed in the SCHEMA CONTEXT below
- Never invent, assume, or infer columns from naming patterns or domain knowledge
- If a required column is absent from the schema, compute the KPI without it and add a SQL comment: -- WARNING: column not found, approximated

OUTPUT FORMAT
- Return ALL KPIs in ONE final SELECT
- Alias each KPI column using the exact name from KPI DEFINITIONS (snake_case, no spaces)
- No SELECT *
- No INSERT / UPDATE / DELETE / DROP / TRUNCATE — SELECT only
- Return ONLY raw SQL — no markdown fences, no explanations, no inline comments unless a WARNING

QUERY STRUCTURE
- Use CTEs (WITH clauses) for all intermediate logic
- Each CTE must have a clear, descriptive name (e.g., daily_orders, active_users)
- Inline subqueries in FROM or WHERE are forbidden — use CTEs instead
- Final SELECT must reference only CTE names, not raw table names

AGGREGATION SAFETY
- Use COUNT(DISTINCT col) wherever duplicate rows are possible
- Before joining two tables, assess fanout risk:
    * If Table A has 1 row per order and Table B has N rows per order, joining directly inflates metrics
    * Always aggregate the higher-grain table into a CTE FIRST, then join the aggregated CTE
- Never join two fact-like tables directly — always aggregate each into a CTE first

JOIN RULES
- Only join when the join is REQUIRED to satisfy a KPI
- Only join on columns that are explicitly present in BOTH tables' schema context
- Never infer join keys from column name similarity alone
- Prefer LEFT JOIN over INNER JOIN unless the KPI semantics require INNER JOIN
- After every join, ask: could this join duplicate rows? If yes, add COUNT(DISTINCT) or pre-aggregate

DATE & FILTER HANDLING
- If date filters are present in ADDITIONAL INFORMATION, apply them exactly as specified
- If no date filter is given, do NOT add one — return all-time totals unless instructed
- Use CAST(col AS DATE) if comparing a timestamp column to a date literal
- Use consistent date formatting: 'YYYY-MM-DD'

NULL HANDLING
- Wrap aggregations with COALESCE where a zero result is preferable to NULL:
    COALESCE(SUM(col), 0), COALESCE(COUNT(DISTINCT col), 0)
- Do NOT wrap nullable dimension columns in COALESCE unless instructed

========================
DECISION FRAMEWORK (run this before writing SQL)
========================

Step 1 — TABLE SELECTION
For each KPI:
  - Which table(s) contain the required columns?
  - What is the grain of each table (1 row per order? per event? per user?)
  - Which table most directly represents the KPI grain?

Step 2 — COLUMN VERIFICATION  
For each column you plan to use:
  - Is this column present in the SCHEMA CONTEXT? (yes/no)
  - If no → do not use it

Step 3 — JOIN NECESSITY CHECK
For each potential join:
  - Is this join required? (would the KPI be wrong without it?)
  - Is the join key present in BOTH tables' schema?
  - Will this join create fanout? (if yes → pre-aggregate first)

Step 4 — AGGREGATION PLAN
  - Which KPIs are counts? sums? averages? distinct counts?
  - At what grain should each CTE aggregate?
  - Will the final SELECT join aggregated CTEs cleanly at 1-to-1?

Step 5 — WRITE
  - Write one CTE per logical unit
  - Final SELECT: one row, all KPIs as aliased columns

Do NOT output this reasoning — use it only to produce the final SQL.

========================
SCHEMA CONTEXT
========================

{schema_context}

========================
KPI DEFINITIONS
========================

{kpis}

========================
ADDITIONAL INFORMATION
(column meanings, flag values, business logic, filters, date ranges, join hints)
========================

{additional_prompt}

========================
OUTPUT
========================

A single SQL query. Raw SQL only. Nothing else.
"""
    completion = client.chat.completions.create(
        model="openai/gpt-5.2",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    sql = completion.choices[0].message.content.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()
    return sql
