import time
from timeit import default_timer as timer

import altair as alt
import folium
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

from gcp import get_service_account_credentials, get_client


def get_live_stations(_client):
    query = (
        f"""select  code_station,
                    date_obs,
                    flood_indicateur,
                    resultat_obs,
                    latitude,
                    longitude,
                    quantile_999,
                    quantile_990,
                    quantile_900,
                    quantile_001,
            from river_observation_prod.hubeau_indicator_latest
                where date_obs >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
                and code_station is not null
            order by date_obs desc
        """

    )
    df = _client.query(query).to_dataframe()
    return df


def get_map(df, quantile):
    locs_map = folium.Map(
        location=[46.856614, 2.3522219],
        zoom_start=6.5, tiles="cartodbpositron",
        zoom_control=True,
        scrollWheelZoom=False,
        prefer_canvas=True
    )

    df['flood_indicateur'] = (df['resultat_obs'] / df[quantile]).clip(
        lower=0, upper=5)
    df = df[df['flood_indicateur'] > 0]

    for i in range(0, len(df)):
        flood_indicateur = df.iloc[i]["flood_indicateur"]

        cmap = plt.get_cmap('RdYlBu_r')
        norm = mcolors.Normalize(vmin=0, vmax=1)
        color = mcolors.to_hex(cmap(norm(flood_indicateur)))

        R = 2
        S = 15
        radius = min(50, R + S * flood_indicateur)
        folium.CircleMarker(
            location=[df.iloc[i]["latitude"], df.iloc[i]["longitude"]],
            radius=radius,
            # color="green",
            # weight=50,
            # opacity=0.05,
            fill_opacity=0.4,
            fill_color=(color),
            # icon=folium.Icon(
            # icon="flag",
            fill=True,
            popup=df.iloc[i]["code_station"],
            stroke=False,

        ).add_to(locs_map)

    return locs_map


def write_station_details(client, selected_station):
    query_q = f"""
        select resultat_obs_elab, date_obs_elab, stations.libelle_commune, stations.code_departement
        from river_observation_prod.hubeau_historical_flatten hf
        join `riverflood-lewagon`.river_observation_prod.d_hubeau_stations stations on hf.code_station = stations.code_station
        where hf.code_station = '{selected_station}'
    """

    query_live = f"""
        select resultat_obs, date_obs
        from river_observation_prod.hubeau_live_recent
        where code_station = '{selected_station}' and grandeur_hydro = 'Q'
    """

    df_q = client.query(query_q).to_dataframe()
    df_live = client.query(query_live).to_dataframe()

    max_resultat_obs_elab = max(df_q["resultat_obs_elab"])
    max_resultat_obs = max(df_live["resultat_obs"])
    max_y = max(max_resultat_obs_elab, max_resultat_obs)

    st.title(f"Station {selected_station} - {df_q['libelle_commune'][0]} ({df_q['code_departement'][0]})")

    chart_q = (
        alt.Chart(
            data=df_q,
            title="Historical water flow rates",
        )
        .mark_line(point=False,
                   color='red',
                   )
        .encode(
            x=alt.X("date_obs_elab:T", axis=alt.Axis(title="Date", format="%b %Y")),
            y=alt.Y("resultat_obs_elab:Q", axis=alt.Axis(title="Flow Rate"),
                    scale=alt.Scale(domain=[0, max_y])),
        )
        .properties(height=500)
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
    quantile999 = df_indicator["quantile_999"][0]
    quantile990 = df_indicator["quantile_990"][0]
    quantile900 = df_indicator["quantile_900"][0]

    text_q_p999 = (
        alt.Chart(
            data=pd.DataFrame({'y': [quantile999]})
        )
        .mark_text(text='Top 99.9%', dx=-450, dy=-10, color='red', fontSize=20)
        .encode(
            y=alt.Y("y:Q")
        )
    )

    line_q_p999 = (
        alt.Chart(
            data=pd.DataFrame({'y': [quantile999]})
        )
        .mark_rule(color='red', strokeDash=[10, 4])
        .encode(
            y=alt.Y("y:Q")
        )
    )

    text_q_p990 = (
        alt.Chart(
            data=pd.DataFrame({'y': [quantile990]})
        )
        .mark_text(text='Top 99%', dx=-450, dy=-10, color='blue', fontSize=20)
        .encode(
            y=alt.Y("y:Q")
        )
    )

    line_q_p990 = (
        alt.Chart(
            data=pd.DataFrame({'y': [quantile990]})
        )
        .mark_rule(color='blue', strokeDash=[10, 4])
        .encode(
            y=alt.Y("y:Q")
        )
    )

    text_q_p900 = (
        alt.Chart(
            data=pd.DataFrame({'y': [quantile900]})
        )
        .mark_text(text='Top 90%', dx=-450, dy=-10, color='green', fontSize=20)
        .encode(
            y=alt.Y("y:Q")
        )
    )

    line_q_p900 = (
        alt.Chart(
            data=pd.DataFrame({'y': [quantile900]})
        )
        .mark_rule(color='green', strokeDash=[10, 4])
        .encode(
            y=alt.Y("y:Q")
        )
    )

    layer_q = alt.layer(
        chart_q,
        text_q_p999,
        line_q_p999,
        text_q_p990,
        line_q_p990,
        text_q_p900,
        line_q_p900
    ).configure_axis(
        labelFontSize=20,
        titleFontSize=30,
    ).configure_title(
        fontSize=30
    )

    st.altair_chart(layer_q, use_container_width=True)

    chart_live = (
        alt.Chart(
            data=df_live,
            title="Live water flow rates",
        )
        .mark_area()
        .encode(
            # day and time (hours minutes)
            x=alt.X("date_obs:T", axis=alt.Axis(title="Date", format="%b %d %H:%M")),
            y=alt.Y("resultat_obs:Q", axis=alt.Axis(title="Flow Rate"),
                    scale=alt.Scale(domain=[0, max_y])),
        )
        .properties(height=500)
    )

    layer_live = alt.layer(
        chart_live,
        text_q_p999,
        line_q_p999,
        text_q_p990,
        line_q_p990,
        text_q_p900,
        line_q_p900
    ).configure_axis(
        labelFontSize=19,
        titleFontSize=30,
    ).configure_title(
        fontSize=30
    )

    st.altair_chart(layer_live, use_container_width=True)


def create_main_page():
    st.set_page_config(layout="wide", initial_sidebar_state='collapsed')

    st.logo("dashboards/images/BlueRiver.png")
    credentials = get_service_account_credentials()
    client = get_client(credentials)
    st.title("Live river flood monitoring")

    auto_refresh = st.sidebar.checkbox("Auto refresh", value=False)
    st.sidebar.write("Last refresh time:",
                     f'{st.session_state.last_refresh_time if "last_refresh_time" in st.session_state else "never"}')

    quantile = st.radio(
        label="Filter by a quantile (historical flood data) :",
        options=["quantile_999", "quantile_990", "quantile_900"],
        # quantile_999 transforms to "Top 99.9%"
        format_func=lambda x: f"Top {int(x.split('_')[1]) / 10}%",
        horizontal=True,
        index=1,

    )

    col1, col2 = st.columns(2)

    with col1:

        df = get_live_stations(client)
        locs_map = get_map(df, quantile)
        st_data = st_folium(locs_map, width=1100, height=1100)

    with col2:
        selected_station = st_data.get("last_object_clicked_popup")
        if selected_station:
            write_station_details(client, selected_station)

    if auto_refresh:
        time.sleep(20)
        st.rerun()


if __name__ == "__main__":
    create_main_page()
