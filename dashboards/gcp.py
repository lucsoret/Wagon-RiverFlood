import os

import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account


def get_service_account_credentials():
    if 'credentials' not in st.session_state:
        private_key = os.getenv('GCP_JSON_PRIVATE_KEY')
        if private_key is None:
            st.error("Environment variable 'GCP_JSON_PRIVATE_KEY' not set")
            st.stop()
        secrets = st.secrets["gcp_service_account"]
        service_account_info = {
            "type": secrets["type"],
            "project_id": secrets["project_id"],
            "private_key_id": secrets["private_key_id"],
            "private_key": private_key,
            "client_email": secrets["client_email"],
            "client_id": secrets["client_id"],
            "auth_uri": secrets["auth_uri"],
            "token_uri": secrets["token_uri"],
            "auth_provider_x509_cert_url": secrets["auth_provider_x509_cert_url"],
            "client_x509_cert_url": secrets["client_x509_cert_url"],
        }
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        # Create credentials using the constructed dictionary and store in session state
        st.session_state.credentials = credentials
    credentials = st.session_state.credentials

    return credentials


def get_client(credentials):
    if "client" not in st.session_state:
        st.session_state.client = bigquery.Client(credentials=credentials, location="EU")
    client = st.session_state.client
    return client
