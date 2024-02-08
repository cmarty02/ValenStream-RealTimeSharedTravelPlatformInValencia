import streamlit as st
import pandas as pd
from google.cloud import bigquery
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import folium_static
import time
from folium import Marker
import base64

st.set_page_config(page_title="¡Bienvenido/a a Streamlit!", page_icon="🚗")
st.title("DASHBOARD BLABLACAR VALENCIA")
st.write("Bienvenido al dashboard de BlaBlaCar para la ciudad de Valencia. Aquí podrás visualizar y analizar datos relacionados con viajes compartidos en la ciudad.")

# Función para obtener datos desde BigQuery
def get_bigquery_data(project_id, dataset_id, table_id):
    # Configura la autenticación de Google Cloud
    client = bigquery.Client(project=project_id)

    # Realiza la consulta a BigQuery
    query = f"""
        SELECT
          geo,
          coche.coche_id,
          coche.coche_index_msg,
          coche.coche_geo,
          coche.coche_datetime,
          coche.coche_latitud,
          coche.coche_longitud,
          coche.coche_ruta,
          usuario.user_id,
          usuario.user_datetime,
          usuario.user_geo,
          usuario.user_geo_fin,
          fin_viaje
        FROM
          `{project_id}.{dataset_id}.{table_id}`,
          UNNEST(coches) AS coche,
          UNNEST(usuarios) AS usuario
        ORDER BY coche.coche_index_msg;
    """

    df = client.query(query).to_dataframe()
    return df


def main():
    # Configura tus detalles de proyecto, conjunto de datos y tabla
    project_id = 'dataproject-blablacar'
    dataset_id = 'dataset_st'
    table_id = 'tabla_st'

    # Recupera los datos de BigQuery
    df = get_bigquery_data(project_id, dataset_id, table_id)
    
    # Muestra el DataFrame en Streamlit
    st.write("Datos de BigQuery:")
    st.write(df)
    
    # Establece las coordenadas del centro de Valencia, España
    valencia_center_coordinates = [39.4699, -0.3763]

    # Contenedor para el mapa
    map_container = st.empty()
    
    # Crea un mapa centrado en Valencia
    mymap = folium.Map(location=valencia_center_coordinates, zoom_start=13)

    while True:
        for i in range(len(df)):
            latitud = float(df.loc[i, 'coche_latitud'])
            longitud = float(df.loc[i, 'coche_longitud'])
            
            icon = folium.Icon(color='red', icon='car', prefix='fa')
            marker = folium.Marker(location=[latitud, longitud], popup=f"Vehicle ID: {df.loc[i, 'coche_id']}", icon=icon).add_to(mymap)
            

            # Convierte el mapa de Folium a HTML y muestra el HTML directamente en Streamlit
            map_html = f'<iframe width="1000" height="500" src="data:text/html;base64,{base64.b64encode(mymap._repr_html_().encode()).decode()}" frameborder="0" allowfullscreen="true"></iframe>'
            map_container.markdown(map_html, unsafe_allow_html=True)

            # Espera 2 segundos antes de la próxima actualización
            time.sleep(2)
    
    

if __name__ == "__main__":
    main()
