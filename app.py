import streamlit as st
import pandas as pd
import pandas.io.sql as sqlio
import altair as alt
import folium
from streamlit_folium import st_folium
from datetime import datetime

from db import conn_str

st.title("Seattle Events")

# Read data from the database
df = sqlio.read_sql_query("SELECT * FROM events", conn_str)

# Chart 1: Category Distribution
st.subheader("1. Category Distribution")
category_chart = alt.Chart(df).mark_bar().encode(
    x=alt.X("count()", title="Number of Events"),
    y=alt.Y("category", sort="-x", title="Event Category"),
    color="category"
).properties(width=600, height=400).interactive()
st.altair_chart(category_chart, use_container_width=True)

# Chart 2: Monthly Event Count
st.subheader("2. Monthly Event Count")
df['date'] = pd.to_datetime(df['date'])
df['month'] = df['date'].dt.month_name()
month_chart = alt.Chart(df).mark_bar().encode(
    x=alt.X("count()", title="Number of Events"),
    y=alt.Y("month", sort=alt.EncodingSortField(field="date:O", order='ascending'), title="Month"),
    color="month"
).properties(width=600, height=400).interactive()
st.altair_chart(month_chart, use_container_width=True)

# Chart 3: Day of the Week Event Count
st.subheader("3. Day of the Week Event Count")
df['day_of_week'] = df['date'].dt.day_name()
day_chart = alt.Chart(df).mark_bar().encode(
    x=alt.X("count()", title="Number of Events"),
    y=alt.Y("day_of_week", sort=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"], title="Day of the Week"),
    color="day_of_week"
).properties(width=600, height=400).interactive()
st.altair_chart(day_chart, use_container_width=True)

# Filter Controls
st.sidebar.title("Filter Controls")

# Dropdown to filter category
selected_category = st.sidebar.selectbox("Select a category", df['category'].unique())

# Get the minimum and maximum dates from the DataFrame
min_date = min(df['date'])
max_date = max(df['date'])

# Date range selector for event date
start_date = st.sidebar.date_input("Select start date", min_value=min_date, max_value=max_date, value=min_date)
end_date = st.sidebar.date_input("Select end date", min_value=min_date, max_value=max_date, value=max_date)

# Filter by location
selected_location = st.sidebar.selectbox("Select a location", df['location'].unique())

# Optional: Filter by weather condition
selected_weather_condition = st.sidebar.selectbox("Select a weather condition", df['weather_condition'].unique())

# Convert start_date and end_date to datetime64[ns, UTC]
start_date = pd.to_datetime(start_date, utc=True)
end_date = pd.to_datetime(end_date, utc=True)

# Convert the DataFrame's 'date' column to the same timezone as start_date and end_date
df['date'] = df['date'].dt.tz_localize(None)  # Remove timezone information if present
df['date'] = df['date'].dt.tz_localize('UTC')  # Add UTC timezone

# Apply filters to the data
filtered_df = df[(df['category'] == selected_category) &
                 (df['date'] >= start_date) &
                 (df['date'] <= end_date) &
                 (df['location'] == selected_location)]

# Display the filtered data
st.subheader("Filtered Events")
st.write(filtered_df)

# Folium Map with filtered locations
st.sidebar.subheader("Map")
m = folium.Map(location=[47.6062, -122.3321], zoom_start=12)

for index, row in filtered_df.iterrows():
    folium.Marker([row['latitude'], row['longitude']], popup=row['venue']).add_to(m)

st_folium(m, width=1200, height=600)
