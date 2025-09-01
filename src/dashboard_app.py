import streamlit as st
import pandas as pd
import plotly.express as px

# load cleaned data
df = pd.read_parquet("data/cleaned/storms_1996_2025.parquet")

st.title("Severe Weather Events Dashboard")

# Sidebar filters
event_filter = st.sidebar.multiselect("Select Event Types", df["EVENT_TYPE"].unique())
year_filter = st.sidebar.slider("Select Year", int(df["YEAR"].min()), int(df["YEAR"].max()), (1996, 2000))

# Apply filters
mask = (df["YEAR"].between(*year_filter)) & (df["EVENT_TYPE"].isin(event_filter) if event_filter else True)
df_filtered = df[mask]

# Overview charts
st.subheader("Event Counts by Type")
counts = df_filtered["EVENT_TYPE"].value_counts().reset_index()
fig1 = px.bar(counts, x='count', y="EVENT_TYPE", labels={"index": "Event Type", "EVENT_TYPE": "Count"})
st.plotly_chart(fig1)

st.subheader("Damages Over Time")
damage_by_year = df_filtered.groupby("YEAR")[["DAMAGE_PROPERTY", "DAMAGE_CROPS"]].sum().reset_index()
fig2 = px.line(damage_by_year, x="YEAR", y=["DAMAGE_PROPERTY", "DAMAGE_CROPS"], title="Damages per Year")
st.plotly_chart(fig2)

st.subheader("Map of Events")
fig3 = px.scatter_mapbox(df_filtered, lat="BEGIN_LAT", lon="BEGIN_LON",
                         color="EVENT_TYPE", hover_name="EVENT_TYPE",
                         zoom=3, height=500)
fig3.update_layout(mapbox_style="carto-positron")
st.plotly_chart(fig3)
