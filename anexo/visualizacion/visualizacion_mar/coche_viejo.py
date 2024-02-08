import streamlit as st
import pandas as pd
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
        license_plate
    FROM
        dataflow-1-411618.blablacar.vehiculos
    ORDER BY
        manufacturing_year
    LIMIT 1
    """

    # Execute the query
    query_job = client.query(query)

    # Get the result
    result = query_job.result().to_dataframe()

    return result

# Streamlit layout
st.title("Matrícula del Coche Más Viejo en la Tabla")

# Load data from BigQuery
oldest_car_license_plate = load_data_from_bigquery()

# Display the license plate of the oldest car
st.write("Matrícula del Coche Más Viejo:")
st.write(oldest_car_license_plate.iloc[0]['license_plate'])
