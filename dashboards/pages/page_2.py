import streamlit as st

st.logo("dashboards/images/BlueRiver.png")
st.markdown("# Page 2 ❄️")
st.sidebar.markdown("# Page 2 ❄️")

x = st.slider('Dates', min_value=0, max_value=10000, step=1000)
y = st.slider('Flow rates', min_value=0.01, max_value=0.10, step=0.01)

st.write(f"x={x} y={y}")
values = np.cumprod(1 + np.random.normal(x, y, (100, 10)), axis=0)
st.line_chart(values)
