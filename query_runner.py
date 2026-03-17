import requests
import time
import pandas as pd
import os
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

REDASH_URL = "https://redash.intermesh.net"
from utils import get_secret

API_KEY = get_secret("API_KEY")
DATA_SOURCE_ID = 8

headers = {
    "Authorization": f"Key {API_KEY}",
    "Content-Type": "application/json"
}

def run_sql(sql: str):
    response = requests.post(
        f"{REDASH_URL}/api/query_results",
        headers=headers,
        json={
            "query": sql,
            "data_source_id": DATA_SOURCE_ID,
            "max_age": 0
        },
        timeout=30
    )

    response.raise_for_status()
    data = response.json()

    if "job" in data:
        job_id = data["job"]["id"]
        retries = 0

        while retries < 120:
            job_response = requests.get(
                f"{REDASH_URL}/api/jobs/{job_id}",
                headers=headers,
                timeout=10
            )

            job_response.raise_for_status()
            job_data = job_response.json()

            status = job_data["job"]["status"]

            if status == 3:
                query_result_id = job_data["job"]["query_result_id"]
                break
            elif status == 4:
                raise Exception(job_data["job"].get("error", "Query failed"))

            time.sleep(1)
            retries += 1
        else:
            raise Exception("Query execution timeout")

    else:
        query_result_id = data["query_result"]["id"]

    final_response = requests.get(
        f"{REDASH_URL}/api/query_results/{query_result_id}.json",
        headers=headers,
        timeout=30
    )

    final_response.raise_for_status()
    final_data = final_response.json()

    rows = final_data["query_result"]["data"]["rows"]
    return pd.DataFrame(rows)
