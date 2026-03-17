import os
def get_secret(key):
    try:
        import streamlit as st
        return st.secrets[key]
    except:
        return os.getenv(key)
