from datetime import datetime
from timeit import default_timer as timer

import numpy as np
import altair as alt
import folium
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

from streamlit_folium import st_folium
from shapely.geometry import box
from shapely.affinity import scale

import geopandas as gpd
from geopandas.tools import sjoin

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
                    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), date_obs, SECOND) AS seconds_since_observation
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
    # Initialize Matplotlib colormap
    cmap = plt.get_cmap('RdYlBu_r')
    norm = mcolors.Normalize(vmin=0, vmax=1)
    th_opacity = 60*60
    for i in range(0, len(df)):


        flood_indicateur = df.iloc[i]["flood_indicateur"]

        seconds_since_observation = df.iloc[i]["seconds_since_observation"]
        clipped_value = max(0, min(seconds_since_observation, th_opacity))
        fill_opacity = 1 - (clipped_value / th_opacity)

        if flood_indicateur == 0:
            continue


        color = mcolors.to_hex(cmap(norm(flood_indicateur)))
        radius = 10+0*flood_indicateur
        folium.CircleMarker(
            location=[df.iloc[i]["latitude_station"], df.iloc[i]["longitude_station"]],
            radius=radius,
            # color="green",
            # weight=50,
            # opacity=0.05,
            fill_opacity=fill_opacity,
            fill_color=color,
            # icon=folium.Icon(
            # icon="flag",
            fill=True,
            popup=df.iloc[i]["code_station"],
            stroke=False,

        ).add_to(locs_map)

    return locs_map

def get_map_grid(df):
    # Create the map centered on a specific location
    locs_map = folium.Map(
        location=[46.856614, 2.3522219],
        zoom_start=6, tiles="cartodbpositron",
        zoom_control=True,
        scrollWheelZoom=False
    )

    # Initialize Matplotlib colormap
    cmap = plt.get_cmap('RdYlBu_r')
    norm = mcolors.Normalize(vmin=0, vmax=1)

    # Define the grid size in degrees (latitude and longitude)
    grid_size = 0.20

    # Calculate grid cell bounds
    min_lat = df['latitude_station'].min()
    max_lat = df['latitude_station'].max()
    min_lon = df['longitude_station'].min()
    max_lon = df['longitude_station'].max()

    # Create a DataFrame for grid cells
    lat_edges = np.arange(min_lat, max_lat + grid_size, grid_size)
    lon_edges = np.arange(min_lon, max_lon + grid_size, grid_size)

    grid_cells = []
    for lat_min, lat_max in zip(lat_edges[:-1], lat_edges[1:]):
        for lon_min, lon_max in zip(lon_edges[:-1], lon_edges[1:]):
            grid_cells.append({
                'lat_min': lat_min,
                'lat_max': lat_max,
                'lon_min': lon_min,
                'lon_max': lon_max
            })

    grid_df = pd.DataFrame(grid_cells)
    grid_gdf = gpd.GeoDataFrame(
        grid_df,
        geometry=[box(row['lon_min'], row['lat_min'], row['lon_max'], row['lat_max']) for _, row in grid_df.iterrows()],
        crs='EPSG:4326'
    )

    # Convert the original DataFrame to a GeoDataFrame
    df_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df['longitude_station'], df['latitude_station']),
        crs='EPSG:4326'
    )

    # Perform spatial join to find which grid cell each point belongs to
    joined_gdf = sjoin(df_gdf, grid_gdf, how='inner', predicate='within')

    # Calculate mean flood_indicateur for each grid cell
    mean_floods = joined_gdf.groupby('index_right')['flood_indicateur'].mean().reset_index()
    mean_floods = mean_floods.rename(columns={'index_right': 'grid_cell_index'})

    # Merge with the grid cells DataFrame
    grid_gdf = grid_gdf.reset_index().merge(mean_floods, left_index=True, right_on='grid_cell_index', how='left')

    # Plot each grid cell on the map
    for _, cell in grid_gdf.iterrows():
        mean_flood = cell['flood_indicateur']
        if pd.isna(mean_flood) or mean_flood <= 0:
            continue

        # Determine the color based on the mean flood_indicateur
        color = mcolors.to_hex(cmap(norm(mean_flood)))

        # Create a polygon for the grid cell
        folium.Rectangle(
            bounds=[
                [cell['lat_min'], cell['lon_min']],
                [cell['lat_max'], cell['lon_max']]
            ],
            color=color,
            fill=True,
            fill_opacity=0.6,
            fill_color=color
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
    second_start = timer()
    locs_map = get_map(df)
    st_data = st_folium(locs_map, width=725)
    end_time = timer()
    st.write(f"this took {end_time - start_time}")
    st.write(f"Compute the upper map took {end_time - second_start}")

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
