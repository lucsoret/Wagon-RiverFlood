import streamlit as st
# import numpy as np
import pandas as pd
import datetime as dt
import altair as alt
import io



def get_historical_site_data(_client, site_station):
    # Use the function to get credentials
    st.write(site_station)

    # query = (
    #     f"""SELECT *
    #     FROM `riverfloood-lewagon.river_observation_dev.hubeau_live_latest`
    #     WHERE code_station = '{site_station}'
    #     limit 1;
    #     """
    # )

    # #date_obs, grandeur_hydro, resultat_obs
    # query
    # df = _client.query(query).to_dataframe()
    # df
    df = pd.read_csv("./dashboards/images/graph_data.csv",delimiter = ";")
    df = df.filter(["grandeur_hydro", "date_obs", "resultat_obs"])
    # df["resultat_obs"] = pd.to_numeric(df["resultat_obs"])
    df["resultat_obs"] = pd.to_numeric(df["resultat_obs"], errors='coerce')
    df_q = df[df.grandeur_hydro == 'Q']
    df_h = df[df.grandeur_hydro == 'H']
    return (df_q, df_h)

def get_client():
    client = st.session_state.client
    return client

def get_graph_df():
    # st.write(st.session_state)
    client = get_client()
    site_station = st.session_state.site_station
    df_q, df_h = get_historical_site_data(client, site_station)
    st.write(df_q.head(14))
    return (df_q, df_h)


if __name__ == "__main__":
    # sel_tbl = get_graph_df()
    # sel_tbl
    pass

st.title("Flow Rates")

df_q, df_h = get_graph_df()
df_q["date_obs"] = pd.to_datetime(df_q["date_obs"])
df_h["date_obs"] = pd.to_datetime(df_h["date_obs"])

st.metric("Maximum recent flow rate", max(df_q["resultat_obs"]))

buffer = io.StringIO()
df_h.info(buf=buffer)
s = buffer.getvalue()
st.text(s)

st.text(f"min is {min(df_q['resultat_obs'])} and max is {max(df_q['resultat_obs'])}")

chart_h = (
        alt.Chart(
            data=df_h,
            title="Water Heights",
        )
        .mark_line(point=True)
        .encode(
            x=alt.X("date_obs:T", axis=alt.Axis(title="Date")),
            y=alt.Y("resultat_obs:Q", axis=alt.Axis(title="Height"), scale=alt.Scale(domain=[min(df_h["resultat_obs"]),max(df_h["resultat_obs"])])),
        )
        .properties(
        width=600,
        height=500
        )
)

line_h = (
        alt.Chart(
            data = pd.DataFrame({'y':[2000]})
            )
        .mark_rule(color='red', strokeDash=[4, 2])
        .encode(
            y=alt.Y("y:Q")
            )
        )

chart_q = (
        alt.Chart(
            data=df_q,
            title="Water Flow Rates",
        )
        .mark_line(point=True)
        .encode(
            x=alt.X("date_obs:T", axis=alt.Axis(title="Date")),
            y=alt.Y2("resultat_obs:Q", axis=alt.Axis(title="Flow Rate"), scale=alt.Scale(domain=[min(df_q["resultat_obs"]),max(df_q["resultat_obs"])])),
        )
        .properties(
        width=600,
        height=500
        )
)


st.altair_chart(chart_h + line_h)
st.altair_chart(chart_h + chart_q)
# st.altair_chart(chart_q)

st.logo("dashboards/images/BlueRiver.png")
