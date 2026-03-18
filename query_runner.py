import requests
import time
import pandas as pd
import os
import streamlit as st
from dotenv import load_dotenv
from requests import HTTPError

load_dotenv()

REDASH_URL = "https://redash.intermesh.net"
from utils import get_secret

DATA_SOURCE_ID = 8


def _build_headers():
    api_key = get_secret("API_KEY")

    if not api_key:
        raise ValueError("Missing API_KEY secret")

    return {
        "Authorization": f"Key {api_key}",
        "Content-Type": "application/json"
    }


def _extract_api_error(response):
    try:
        payload = response.json()
    except ValueError:
        payload = None

    if isinstance(payload, dict):
        for key in ("message", "error", "errors"):
            value = payload.get(key)
            if value:
                return str(value)

        job = payload.get("job")
        if isinstance(job, dict) and job.get("error"):
            return str(job["error"])

    response_text = response.text.strip()
    return response_text or response.reason

def run_sql(sql: str):
    headers = _build_headers()

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

    try:
        response.raise_for_status()
    except HTTPError as exc:
        error_message = _extract_api_error(response)
        raise RuntimeError(
            f"Redash query submission failed ({response.status_code}): {error_message}"
        ) from exc

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

            try:
                job_response.raise_for_status()
            except HTTPError as exc:
                error_message = _extract_api_error(job_response)
                raise RuntimeError(
                    f"Redash job polling failed ({job_response.status_code}): {error_message}"
                ) from exc

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

    try:
        final_response.raise_for_status()
    except HTTPError as exc:
        error_message = _extract_api_error(final_response)
        raise RuntimeError(
            f"Redash result fetch failed ({final_response.status_code}): {error_message}"
        ) from exc

    final_data = final_response.json()

    rows = final_data["query_result"]["data"]["rows"]
    return pd.DataFrame(rows)
