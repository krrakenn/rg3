import streamlit as st
import pandas as pd
from sheets_automation2 import automate_report
from sql_generator import generate_sql_chat_response, merge_queries_llm
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


def initialize_session_state():
    defaults = {
        "result": None,
        "sql": None,
        "last_error": None,
        "last_attempted_sql": None,
        "kpi_chat_messages": [],
        "kpi_sql_draft": "",
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def reset_execution_state():
    st.session_state.result = None
    st.session_state.sql = None
    st.session_state.last_error = None
    st.session_state.last_attempted_sql = None


def reset_kpi_chat_state():
    reset_execution_state()
    st.session_state.kpi_chat_messages = []
    st.session_state.kpi_sql_draft = ""


def append_kpi_chat_message(role, content):
    if content and str(content).strip():
        st.session_state.kpi_chat_messages.append({
            "role": role,
            "content": str(content).strip()
        })


def clear_kpi_draft():
    st.session_state.kpi_sql_draft = ""
    reset_execution_state()


# -------------------------
initialize_session_state()


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
    reset_kpi_chat_state()
    st.session_state.last_mode = mode_sql

st.divider()


# ======================================================
# KPI MODE
# ======================================================

if not mode_sql:

    with st.sidebar:

        st.header("Session")

        st.divider()

        st.caption(
            "Chat history and SQL drafts stay isolated to this Streamlit session."
        )

        if st.button("Clear Chat", use_container_width=True):
            reset_kpi_chat_state()
            st.rerun()

    col1, col2 = st.columns([1, 2], gap="large")

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
        st.markdown("### Report Chat")
        st.caption("Describe the report, refine it in chat, edit the SQL draft, and run it as many times as needed.")

        if not st.session_state.kpi_chat_messages:
            st.info("Start by selecting tables, then describe the report you want to build.")

        for message in st.session_state.kpi_chat_messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

        chat_prompt = st.chat_input(
            "Ask for KPIs, filters, joins, or revisions",
            disabled=not selected_tables,
            key="kpi_chat_input"
        )

        if chat_prompt:
            if not selected_tables:
                st.warning("Please select at least one table before starting the chat")
            else:
                schema_context = build_schema_context(schema_df, selected_tables)
                append_kpi_chat_message("user", chat_prompt)

                try:
                    with st.status("Updating SQL draft", expanded=True) as status:
                        status.write("Reviewing the chat context")
                        response = generate_sql_chat_response(
                            schema_context,
                            selected_tables,
                            st.session_state.kpi_chat_messages,
                            current_sql=st.session_state.kpi_sql_draft or None
                        )
                        status.write("Refreshing the SQL draft")
                        status.update(label="SQL draft updated", state="complete")

                    append_kpi_chat_message("assistant", response["assistant_message"])
                    st.session_state.kpi_sql_draft = response["sql"]
                    reset_execution_state()
                    st.rerun()
                except Exception as exc:
                    append_kpi_chat_message("assistant", f"I could not update the SQL draft: {exc}")
                    st.session_state.last_error = str(exc)
                    st.rerun()

        st.markdown("### Current SQL Draft")

        st.text_area(
            "Edit the SQL before running it",
            height=260,
            key="kpi_sql_draft",
            placeholder="Your SQL draft will appear here after the chat generates it."
        )

        st.markdown("### SQL Preview")
        st.code(
            st.session_state.kpi_sql_draft or "-- Your SQL draft will appear here after the chat generates it.",
            language="sql"
        )

        action_col1, action_col2 = st.columns(2)

        with action_col1:
            run_kpi_query = st.button("Run Query", use_container_width=True, type="primary")

        with action_col2:
            st.button(
                "Clear Draft",
                use_container_width=True,
                on_click=clear_kpi_draft
            )

        if run_kpi_query:
            draft_sql = st.session_state.kpi_sql_draft.strip()

            if not draft_sql:
                st.warning("Generate or enter a SQL draft before running the query")
            else:
                reset_execution_state()
                st.session_state.last_attempted_sql = draft_sql

                try:
                    with st.status("Executing SQL query", expanded=True) as status:
                        status.write("Sending the SQL draft to Redash")
                        result = run_sql(draft_sql)
                        status.update(label="Execution completed", state="complete")

                    st.session_state.result = result
                    st.session_state.sql = draft_sql
                    st.success("Query executed successfully")
                except Exception as exc:
                    st.session_state.last_error = str(exc)
                    st.error(str(exc))


# ======================================================
# SQL MODE
# ======================================================

else:

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
                        query_type=query_type
                    )
                
                st.success(f"✅ Report automated successfully! (ID: {result['automation_id']})")
                st.json(result)
                
            except Exception as e:
                st.error(f"❌ Failed to automate report: {str(e)}")
