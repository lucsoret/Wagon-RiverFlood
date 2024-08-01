from datetime import datetime
import streamlit as st

from dashboards.gcp import get_service_account_credentials, get_client


def get_latest_rolling_data(_client):
    current_live_list = st.session_state.current_live_df.to_list() if "current_live_df" in st.session_state else []

    now = datetime.now()

    where_clause = f"where date_obs > '{now}'" if current_live_list else ""

    query = (
        f"""
        select code_station, code_site, date_obs, resultat_obs, grandeur_hydro,
        from `river_observation_prod.hubeau_live_latest`
        {where_clause}
        order by date_obs desc limit 20
        """
    )

    # no refresh time at first
    # read it from session state or date_obs

    df = _client.query(query).to_dataframe()

    st.session_state.last_refresh_time = now

    return df


def create_live_page():
    credentials = get_service_account_credentials()
    client = get_client(credentials)

    st.subheader("Latest live data")
    live_df = get_latest_rolling_data(client)

    st.write(live_df)
