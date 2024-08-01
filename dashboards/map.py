from datetime import datetime
from timeit import default_timer as timer

import altair as alt
import folium
import pandas as pd
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
                    quantile_999,
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
        quantile_999 = df.iloc[i]["quantile_999"]
        quantile_990 = df.iloc[i]["quantile_990"]
        quantile_900 = df.iloc[i]["quantile_900"]
        quantile_001 = df.iloc[i]["quantile_001"]
        resultat_obs = df.iloc[i]["resultat_obs"]

        flood_indicateur = (
                                   resultat_obs / quantile_990) / 10  # to match what we said on the bottom regarding quantiles being divided by 10

        if flood_indicateur == 0:
            continue

        color = "red" if flood_indicateur > 1 else "blue"

        radius = flood_indicateur * 20 if flood_indicateur > 1 else flood_indicateur
        folium.CircleMarker(
            location=[df.iloc[i]["latitude_station"], df.iloc[i]["longitude_station"]],
            radius=radius,
            # color="green",
            # weight=50,
            # opacity=0.05,
            fill_opacity=0.2,
            fill_color=(color),
            # icon=folium.Icon(
            # icon="flag",
            fill=True,
            popup=df.iloc[i]["code_station"],
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

    selected_station = st_data.get("last_object_clicked_popup")
    if selected_station:
        write_station_details(client, selected_station)


def write_station_details(client, selected_station):
    query_q = f"""
        select resultat_obs_elab, date_obs_elab
        from river_observation_prod.hubeau_historical_flatten
        where code_station = '{selected_station}'
    """

    df_q = client.query(query_q).to_dataframe()

    chart_q = (
        alt.Chart(
            data=df_q,
            title="Water Flow Rates to calculate P90",
        )
        .mark_line(point=False,
                   color='red',
                   strokeDash=[4, 2])
        .encode(
            x=alt.X("date_obs_elab:T", axis=alt.Axis(title="Date")),
            y=alt.Y("resultat_obs_elab:Q", axis=alt.Axis(title="Flow Rate"),
                    scale=alt.Scale(domain=[min(df_q["resultat_obs_elab"]), max(df_q["resultat_obs_elab"])])),
        )
    )

    latest_station_indicator_query = f"""
        select resultat_obs,
               quantile_999,
               quantile_990,
               quantile_900,
        from river_observation_prod.hubeau_indicator_latest indicators
        where code_station = '{selected_station}'
    """

    df_indicator = client.query(latest_station_indicator_query).to_dataframe()
    quantile999 = df_indicator["quantile_999"][0] * 10
    quantile990 = df_indicator["quantile_990"][0] * 10
    quantile900 = df_indicator["quantile_900"][
                      0] * 10  # We've multiply this by 10 because indicators_latest had it divided by 10

    line_q_p999 = (
        alt.Chart(
            data=pd.DataFrame({'y': [quantile999]})
        )
        .mark_rule(color='red', strokeDash=[4, 2])
        .encode(
            y=alt.Y("y:Q")
        )
    )

    line_q_p990 = (
        alt.Chart(
            data=pd.DataFrame({'y': [quantile990]})
        )
        .mark_rule(color='blue', strokeDash=[4, 2])
        .encode(
            y=alt.Y("y:Q")
        )
    )

    line_q_p900 = (
        alt.Chart(
            data=pd.DataFrame({'y': [quantile900]})
        )
        .mark_rule(color='green', strokeDash=[4, 2])
        .encode(
            y=alt.Y("y:Q")
        )
    )

    st.altair_chart(chart_q + line_q_p999 + line_q_p990 + line_q_p900, use_container_width=True)

    query_live = f"""
        select resultat_obs, date_obs
        from river_observation_prod.hubeau_live_recent
        where code_station = '{selected_station}' and grandeur_hydro = 'Q'
    """

    df_live = client.query(query_live).to_dataframe()

    chart_live = (
        alt.Chart(
            data=df_live,
            title="Live Water Flow Rates",
        )
        .mark_line(point=True)
        .encode(
            x=alt.X("date_obs:T", axis=alt.Axis(title="Date")),
            y=alt.Y("resultat_obs:Q", axis=alt.Axis(title="Flow Rate"),
                    scale=alt.Scale(domain=[min(df_live["resultat_obs"]), max(df_live["resultat_obs"])])),
        )
    )

    st.altair_chart(chart_live, use_container_width=True)


if __name__ == "__main__":
    create_main_page()
