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
You are a senior analytics engineer writing production-safe SQL.

Your task is to generate ONE SQL query for the requested KPIs using ONLY the provided schema context.

Non-negotiable rules:
1. Use only tables from im_dwh_rpt and only columns present in the schema context.
2. Do not invent columns, relationships, filters, dimensions, or business logic.
3. Do not guess joins.
4. Join tables only when:
   - the join is necessary for the KPI, and
   - the join keys are explicitly visible in the schema context, and
   - the join will not create obvious fanout risk.
5. If the schema is ambiguous, prefer the safest single-table query instead of a risky multi-table join.
6. Never directly join multiple fact-like tables unless the join path is explicitly stated in the instructions.
7. If combining multiple sources is necessary, aggregate each source in a CTE first, then join the aggregated CTEs.
8. Prefer COUNT(DISTINCT ...) when duplicate expansion is possible.
9. Return all KPIs in one final row.
10. Alias each KPI exactly as requested.
11. Return only SQL. No explanation, no comments, no markdown.

Decision policy:
- First determine the base table for each KPI.
- Use dimension tables only for descriptive enrichment or filters.
- If two candidate tables look similar, choose the one that most directly matches the KPI grain.
- If a join is not explicit, do not infer it from naming similarity alone.

Required SQL style:
- Use CTEs for intermediate logic.
- No SELECT *.
- Fully qualify all table names as im_dwh_rpt.table_name.
- Keep the query minimal and deterministic.

SCHEMA CONTEXT:
{schema_context}

KPI DEFINITIONS:
{kpis}

ADDITIONAL INSTRUCTIONS:
{additional_prompt}

Return only one SQL query.
"""

    completion = client.chat.completions.create(
        model="openai/gpt-5.2",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    sql = completion.choices[0].message.content.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()
    return sql
