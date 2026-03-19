import streamlit as st
import pandas as pd
from datetime import date
from sheets_automation2 import automate_report
from sql_generator import generate_sql, merge_queries_llm
from query_runner import run_sql

st.set_page_config(
    page_title="Report Generator",
    layout="wide"
)

MAX_RETRIES = 3


@st.cache_data
def load_schema():
    df = pd.read_csv("restructured_schema.csv")
    return df


schema_df = load_schema()
all_tables = list(schema_df.columns)


def build_schema_context(schema_df, selected_tables):

    schema_text = ""

    for table in selected_tables:

        schema_text += f"\nTable: {table}\nColumns:\n"

        cols = schema_df[table].dropna().tolist()

        for col in cols:
            schema_text += f"- {col}\n"

    return schema_text


# -------------------------
# SESSION STATE
# -------------------------
if "result" not in st.session_state:
    st.session_state.result = None

if "sql" not in st.session_state:
    st.session_state.sql = None

if "last_error" not in st.session_state:
    st.session_state.last_error = None

if "last_attempted_sql" not in st.session_state:
    st.session_state.last_attempted_sql = None


# -------------------------
# UI STYLING
# -------------------------
st.markdown("""
<style>
.stApp {
    background-color: #0E1117;
}

section[data-testid="stSidebar"] {
    background-color: #161B22;
}

h1, h2, h3, h4 {
    font-weight: 600;
}

.block-container {
    padding-top: 2rem;
    padding-bottom: 2rem;
}
</style>
""", unsafe_allow_html=True)


title_col, toggle_col = st.columns([10,2])

with title_col:
    st.title("Report Generator")
    st.caption("Generate reports from tables and KPI definitions")

with toggle_col:
    st.markdown("### ")
    mode_sql = st.toggle("I have Query")

if "last_mode" not in st.session_state:
    st.session_state.last_mode = mode_sql

if st.session_state.last_mode != mode_sql:
    st.session_state.result = None
    st.session_state.sql = None
    st.session_state.last_error = None
    st.session_state.last_attempted_sql = None
    st.session_state.last_mode = mode_sql

st.divider()


# ======================================================
# KPI MODE
# ======================================================

if not mode_sql:

    with st.sidebar:

        st.header("Settings")

        MAX_RETRIES = st.number_input(
            "Maximum Retry Attempts",
            min_value=1,
            max_value=5,
            value=3
        )

        st.divider()

        st.caption(
            "If SQL fails, the system retries by providing error feedback to the model."
        )

    col1, col2, col3 = st.columns(3, gap="large")

    with col1:

        st.markdown("### Select Tables")

        selected_tables = st.multiselect(
            "Choose tables",
            options=all_tables,
            placeholder="Search tables from im_dwh_rpt"
        )

        if selected_tables:

            with st.expander("Preview Schema"):

                preview_context = build_schema_context(schema_df, selected_tables)

                st.text_area(
                    "Schema Preview",
                    preview_context,
                    height=220,
                    disabled=True
                )

    with col2:

        st.markdown("### KPI Definitions")

        kpis = st.text_area(
            "Define KPIs",
            height=200,
            placeholder="""
Example:
Daily revenue
New users per day
Top categories by sales
"""
        )

    with col3:

        st.markdown("### Prompt Box")

        additional_prompt = st.text_area(
            "Column meanings, flag values, filters, joins and etc.",
            height=200,
            placeholder="""""")

    run = st.button("Generate Report", use_container_width=True,type="primary")


    # ---------------------------------
    # GENERATE SQL
    # ---------------------------------

    if run:

        st.session_state.last_error = None
        st.session_state.last_attempted_sql = None

        if not selected_tables:
            st.warning("Please select at least one table")
            st.stop()

        if not kpis:
            st.warning("KPI definitions are required")
            st.stop()

        schema_context = build_schema_context(schema_df, selected_tables)

        attempt = 0
        last_error = None
        sql = None

        status = st.status("Running SQL generation", expanded=True)

        while attempt < MAX_RETRIES:

            attempt += 1

            status.write(f"Attempt {attempt}: generating SQL")

            if last_error:

                prompt_kpi = f"""
KPIs:
{kpis}

Additional Instructions:
{additional_prompt}

Previous SQL:
{sql}

Previous SQL Error:
{last_error}

Fix the SQL.
Return only SQL.
"""

                sql = generate_sql(schema_context, prompt_kpi, "")

            else:

                sql = generate_sql(schema_context, kpis, additional_prompt)

            try:

                status.write("Executing SQL query")
                st.session_state.last_attempted_sql = sql

                result = run_sql(sql)

                status.update(label="Execution completed", state="complete")

                st.session_state.result = result
                st.session_state.sql = sql
                st.session_state.last_error = None

                st.success(f"Query succeeded on attempt {attempt}")

                break

            except Exception as e:

                last_error = str(e)
                st.session_state.last_error = last_error

                status.write(f"Attempt {attempt} failed")
                status.write(last_error)

        else:

            status.update(label="Execution failed", state="error")
            st.error("All retry attempts failed")


# ======================================================
# SQL MODE
# ======================================================

else:

    st.subheader("SQL Query Mode")

    query_count = st.number_input(
        "Number of Queries",
        min_value=1,
        max_value=5,
        value=1
    )

    queries = []

    for i in range(query_count):

        q = st.text_area(
            f"Query {i+1}",
            height=160,
            key=f"query_{i}"
        )

        queries.append(q)

    run_sql_mode = st.button("Run Queries", use_container_width=True,type="primary")

    if run_sql_mode:

        st.session_state.last_error = None
        st.session_state.last_attempted_sql = None

        valid_queries = [q for q in queries if q.strip()]

        if not valid_queries:
            st.warning("Please enter at least one SQL query")
            st.stop()

        with st.spinner("Merging queries..."):
            merged_sql = merge_queries_llm(valid_queries)
        with st.spinner("Executing Query..."):
            attempt = 0
            last_error = None
            sql = merged_sql

            while attempt < MAX_RETRIES:

                attempt += 1

                try:

                    st.session_state.last_attempted_sql = sql

                    result = run_sql(sql)

                    st.session_state.result = result
                    st.session_state.sql = sql
                    st.session_state.last_error = None

                    break

                except Exception as e:

                    last_error = str(e)
                    st.session_state.last_error = last_error

                    prompt = f"""
Queries:
{valid_queries}

Previous SQL:
{sql}

Previous SQL Error:
{last_error}

Fix the SQL.
Return only SQL.
"""

                    sql = merge_queries_llm([prompt])

            else:

                st.error("All retry attempts failed")


if st.session_state.last_error and st.session_state.result is None:
    with st.expander("Last execution error", expanded=True):
        st.error(st.session_state.last_error)
        if st.session_state.last_attempted_sql:
            st.code(st.session_state.last_attempted_sql, language="sql")


# ======================================================
# SHOW RESULT
# ======================================================

if st.session_state.result is not None:

    tab1, tab2 = st.tabs(["Result", "Generated SQL"])

    with tab1:
        st.dataframe(st.session_state.result, use_container_width=True)

    with tab2:
        st.code(st.session_state.sql, language="sql")

    st.divider()

    st.markdown("#### If Everything Looks Good, Automate this Report!")

    generate_report = st.checkbox("Automate Report")

    if generate_report:

        col_gs1, col_gs2, col_gs3 = st.columns([2, 1, 1])

        with col_gs1:
            sheet_url = st.text_input(
                "Google Sheet URL",
                placeholder="https://docs.google.com/spreadsheets/d/..."
            )

        with col_gs2:
            refresh_freq = st.selectbox(
                "Refresh Frequency",
                ["daily", "weekly", "monthly"]
            )

        with col_gs3:
            has_date_filter = st.selectbox(
                "Has Date Filter?",
                ["No", "Yes"]
            )
            query_type = "with_date" if has_date_filter == "Yes" else "no_date"

        schedule_start_date = st.date_input(
            "Schedule Start Date",
            value=date.today(),
            help="The scheduler will start recurring runs from this date. Example: choose a Monday for weekly Monday runs."
        )

        st.info("Grant Editor Access to the below service account:")
        st.code("streamlit-sheets-bot@streamlit-audit-dashboard.iam.gserviceaccount.com")
        
        if st.button("Automate Report", use_container_width=True, type="primary"):
            
            if not sheet_url:
                st.error("Please enter a Google Sheet URL")
                st.stop()
            
            try:
                with st.spinner("Setting up automation..."):
                    result = automate_report(
                        sheet_url=sheet_url,
                        result_df=st.session_state.result,
                        sql_query=st.session_state.sql,
                        refresh_frequency=refresh_freq,
                        query_type=query_type,
                        schedule_start_date=schedule_start_date
                    )
                
                st.success(f"✅ Report automated successfully! (ID: {result['automation_id']})")
                st.json(result)
                
            except Exception as e:
                st.error(f"❌ Failed to automate report: {str(e)}")
