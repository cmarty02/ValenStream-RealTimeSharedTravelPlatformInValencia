import streamlit as st
import altair as alt
import pandas as pd
import json

# Leer el archivo JSON
with open('datos.json', 'r') as archivo:
    datos = json.load(archivo)

# Convertir el diccionario del archivo JSON a un DataFrame de pandas
df = pd.DataFrame(datos['coches'])

# Crear el heatmap con Altair
heatmap = alt.Chart(df).mark_rect().encode(
    x='coche_longitud:Q',
    y='coche_latitud:Q',
    color=alt.value('steelblue')
).properties(
    width=800,
    height=600
)

# Mostrar el heatmap en Streamlit
st.write(heatmap)
