from datetime import datetime
from timeit import default_timer as timer

import folium
import streamlit as st
from streamlit_folium import st_folium

from gcp import get_service_account_credentials, get_client


def get_latest_rolling_data(_client):
    current_live_list = st.session_state.current_live_df.to_list() if "current_live_df" in st.session_state else []

    now = datetime.now()

    where_clause = f"where date_obs > '{now}'" if current_live_list else ""

    query = (
        f"""
        select code_station, code_site, date_obs, resultat_obs, grandeur_hydro,
        from `river_observation_prod.hubeau_live_latest`
        {where_clause}
        order by date_obs desc limit 5
        """
    )

    # no refresh time at first
    # read it from session state or date_obs

    df = _client.query(query).to_dataframe()

    st.session_state.last_refresh_time = now

    return df


def get_live_stations(_client):
    query = (
        f"""select  stations.code_station,
                    date_obs,
                    flood_indicateur,
                    resultat_obs,
                    stations.latitude_station,
                    stations.longitude_station,
                    quantile_990,
                    quantile_900,
                    quantile_001,
            from river_observation_prod.hubeau_indicator_latest indicators
                join river_observation_prod.d_hubeau_stations stations
                    on indicators.code_station = stations.code_station
            order by date_obs desc
        """

    )
    df = _client.query(query).to_dataframe()
    return df


def get_map(df):
    locs_map = folium.Map(
        location=[46.856614, 2.3522219],
        zoom_start=6, tiles="cartodbpositron",
        zoom_control=True,
        scrollWheelZoom=False
    )

    for i in range(0, len(df)):
        quantile_990 = df.iloc[i]["quantile_990"]
        quantile_900 = df.iloc[i]["quantile_900"]
        quantile_001 = df.iloc[i]["quantile_001"]
        resultat_obs = df.iloc[i]["resultat_obs"]

        flood_indicateur = resultat_obs / quantile_900 / 10

        if flood_indicateur == 0:
            continue

        color = "red" if flood_indicateur > 0.9 else "blue"

        folium.CircleMarker(
            location=[df.iloc[i]["latitude_station"], df.iloc[i]["longitude_station"]],
            radius=flood_indicateur ** 2,
            # color="green",
            # weight=50,
            # opacity=0.05,
            fill_opacity=1,
            fill_color=(color),
            # icon=folium.Icon(
            # icon="flag",
            fill=True,
            popup=[df.iloc[i]["code_station"], flood_indicateur],
            stroke=False,

        ).add_to(locs_map)

    return locs_map


def create_main_page():
    st.logo("dashboards/images/BlueRiver.png")
    start_time = timer()
    credentials = get_service_account_credentials()
    client = get_client(credentials)
    st.title("River flood")

    st.subheader("Latest live data")
    st.metric("Last refresh time:",
              f'{st.session_state.last_refresh_time if "last_refresh_time" in st.session_state else "never"}')
    live_df = get_latest_rolling_data(client)

    st.write(live_df)

    df = get_live_stations(client)
    locs_map = get_map(df)
    st_data = st_folium(locs_map, width=725)
    end_time = timer()
    st.write(f"this took {end_time - start_time}")
    st.write(f"site station set to {st_data['last_object_clicked_popup']}")

    # # return

    st.session_state.site_station = st_data["last_object_clicked_popup"]

    return


if __name__ == "__main__":
    create_main_page()
    #
    # while True:
    #     time.sleep(30)
    #     st.rerun()
