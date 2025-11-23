"""
CDC Hospital Admissions Dashboard - Streamlit Application

Interactive web dashboard for visualizing CDC hospital admissions data.
Connects to BigQuery to display:
- COVID-19 admission trends over time
- Inpatient bed utilization by county
- Admission level indicators

Note: Update the BigQuery query with your actual project and dataset names.
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
import plotly.express as px

# ----------------------------------------
# CONFIGURE STREAMLIT APPLICATION
# ----------------------------------------
# Sets up the page title and wide layout for better dashboard visualization
st.set_page_config(page_title="CDC Hospital Admissions", layout="wide")
st.title("CDC Hospital Admissions Dashboard")

# ----------------------------------------
# CONNECT TO BIGQUERY AND FETCH DATA
# ----------------------------------------
# Establishes connection to BigQuery and queries the processed hospital admissions data
# TODO: Update the query with your actual project and dataset names
client = bigquery.Client()

query = """
SELECT state, county, week_end_date, total_adm_all_covid_confirmed, 
       avg_percent_inpatient_beds, admissions_covid_confirmed_level
FROM `YOUR_PROJECT.YOUR_DATASET.cdc_hospital_admissions`
ORDER BY week_end_date DESC
LIMIT 1000
"""

df = client.query(query).to_dataframe()

# ----------------------------------------
# DASHBOARD VISUALIZATION COMPONENTS
# ----------------------------------------

# State filter: Allows users to filter data by state for focused analysis
states = df['state'].unique()
selected_state = st.selectbox("Select State", states)
df_state = df[df['state'] == selected_state]

# Time series chart: Shows COVID-19 admission trends over time for selected state
fig1 = px.line(df_state, x='week_end_date', y='total_adm_all_covid_confirmed',
               title=f'Total COVID Confirmed Admissions in {selected_state}')
st.plotly_chart(fig1, use_container_width=True)

# Bar chart: Displays average inpatient bed utilization by county
fig2 = px.bar(df_state, x='county', y='avg_percent_inpatient_beds',
              title=f'Avg % Inpatient Beds by County in {selected_state}')
st.plotly_chart(fig2, use_container_width=True)

# Data table: Shows detailed admission level information by county
st.subheader("COVID Admissions Level")
st.dataframe(df_state[['county', 'admissions_covid_confirmed_level', 'total_adm_all_covid_confirmed']])
