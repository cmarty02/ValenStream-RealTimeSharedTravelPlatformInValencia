import streamlit as st
import pandas as pd
import altair as alt
from google.cloud import bigquery

# Function to load data from BigQuery
def load_data_from_bigquery():
    # Authenticate to Google Cloud
    # You can skip this step if you're running the script on Google Colab or a GCP environment
    # Otherwise, make sure you have set up authentication properly
    # See https://cloud.google.com/docs/authentication/getting-started for more details
    client = bigquery.Client()

    # Query your data from BigQuery
    query = """
    SELECT
        car_brand,
        COUNT(*) AS count
    FROM
        dataflow-1-411618.blablacar.vehiculos
    GROUP BY
        car_brand
    ORDER BY
        COUNT(*) ASC
    """

    # Execute the query
    query_job = client.query(query)

    # Convert the results to a DataFrame
    results = query_job.result().to_dataframe()

    return results

# Function to create ascending brand evolution chart
def brand_evolution_chart(data):
    chart = alt.Chart(data).mark_line().encode(
        x='car_brand',
        y='count'
    ).properties(
        width=600,
        height=400
    )
    st.altair_chart(chart, use_container_width=True)

# Streamlit layout
st.title("Evolución Ascendente de Marcas de Coche")

# Load data from BigQuery
data = load_data_from_bigquery()

# LAYING OUT THE MIDDLE SECTION OF THE APP WITH THE CHART
st.write("**Evolución Ascendente de Marcas de Coche**")
brand_evolution_chart(data)
