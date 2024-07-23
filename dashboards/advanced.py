import altair as alt
import pandas as pd
import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from streamlit_folium import st_folium
import folium
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

@st.cache_data(ttl=600)
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache_data to hash the return value.
    rows = [dict(row)["data"] for row in rows_raw]
    return pd.DataFrame(rows[18])

def plot_map(df):

    locs_map = folium.Map(
        location=[46.856614, 2.3522219],
        zoom_start=6, tiles="cartodbpositron",
        zoom_control=False,
        scrollWheelZoom=False
    )

    for i in range(0,len(df)):
        folium.Marker(
            location=[df.iloc[i]["latitude"], df.iloc[i]["longitude"]],
            popup=df.iloc[i]["code_station"],
            icon=folium.Icon(
                icon="flag",
                color=("ed" if df.iloc[i]["resultat_obs_elab"] > 1000 else "blue"))
        ).add_to(locs_map)
    st_data = st_folium(locs_map, width=725)



def create_main_page():
    """
    Creates the following Streamlit headers:
    - A title
    - A subheader
    - A title in the sidebar
    - A markdown section in the sidebar
    - A widget in the sidebar to select a table from the `TABLES` list,
    and then return the selected table (instead of the hard-coded "races" value)
    """
    st.title("River flood")

    df = run_query(
        f"""SELECT data
        FROM `riverflood-lewagon.river_observation_multiregion.hubeau_historical`
        limit 20;
        """
    )
    # Print results.
    plot_map(df)
    return

def session_state(data):
    """
    Initialize the session state
    using data as the key and value as the
    initialization value.

    Put data in the session state after having
    initialized it.

    Args:
        data (pd.DataFrame): The formula 1 dataset
    """

    # Initialization of session state, assign a random value
    # to the session state
    if "data" not in st.session_state:
        st.session_state.data = pd.DataFrame()
    else:
        st.session_state.data = data

if __name__ == "__main__":
    selected_table = create_main_page()
