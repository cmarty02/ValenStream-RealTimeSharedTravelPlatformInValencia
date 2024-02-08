import streamlit as st
import pandas as pd
import plotly.express as px
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
        license_plate,
        smoking
    FROM
        dataflow-1-411618.blablacar.vehiculos
    ORDER BY
        license_plate
    """

    # Execute the query
    query_job = client.query(query)

    # Convert the results to a DataFrame
    results = query_job.result().to_dataframe()

    return results

# Streamlit layout
st.title("Distribución de Coches Fumadores y No Fumadores por Matrícula")

# Load data from BigQuery
data = load_data_from_bigquery()

# Create a bar chart using Plotly
fig = px.bar(data, x='license_plate', color='smoking', title='Distribución de Coches Fumadores y No Fumadores por Matrícula')
fig.update_layout(xaxis_title='Matrícula', yaxis_title='Cantidad', legend_title='Fumador')
st.plotly_chart(fig, use_container_width=True)