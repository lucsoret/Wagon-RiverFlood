import streamlit as st
from streamlit_player import st_player

def vid():

    st.logo("dashboards/images/BlueRiver.png")
    st.markdown("# Wrath of the rivers")
    st.sidebar.markdown("# danger")

    st_player("https://youtu.be/o-5tex2qZLU")

if __name__=="__main__":
    vid
