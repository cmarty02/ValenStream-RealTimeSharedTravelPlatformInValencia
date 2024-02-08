
import streamlit as st
import folium
from streamlit_folium import folium_static
from google.cloud import bigquery
import random

#################################################### Adriana ###################################################
#project_id = 'woven-justice-411714'
#topic_name= 'blablacar_DataProject2'
#tabla_name = 'woven-justice-411714.ejemplo.coches'

#################################################### Cris ######################################################
#project_id = 'dataflow-1-411618'
#topic_name= 'coches'
#tabla_name = 'dataflow-1-411618.blablacar.rutas'
#################################################### Jesús #####################################################
project_id = 'blablacar-412022'
topic_name= 'coches'
tabla_name = 'blablacar-412022.dataset.coches'

#Configuramos la página para que ocupe la anchura completa del navegador
st.set_page_config(layout="wide")

logo_url="https://user-images.githubusercontent.com/8149019/166203710-737d477f-c325-4417-8518-7b378918d1f1.png"
st.image(logo_url, width=40)


#Creamos dos pestañas para las distintas visualizaciones que necesitamos
tab1, tab2 = st.tabs(["En directo", "Métricas"])

client = bigquery.Client()

def leer_datos_bigquery(tabla):
    # Agrega comillas inversas alrededor del nombre de la tabla
    query = f"SELECT coche_id, index_msg, longitud, latitud, datetime, ruta FROM `{tabla_name}` ORDER BY index_msg ASC "  
    return client.query(query).to_dataframe()

def coches_totales(tabla_coches_totales):
    query = f"SELECT COUNT(DISTINCT coche_id) as coches_totales FROM `{tabla_name}`"

def coches_dia(tabla_coches_dia):
    query = f"SELECT DATE(datetime) as fecha, COUNT(DISTINCT coche_id) as coches_dia FROM `{tabla_name}` GROUP BY fecha"



# Función para crear un mapa de Folium con la ruta y colores diferentes por coche_id
def crear_mapa_folium(datos, ruta_seleccionada=None):
    datos.rename(columns={'longitud': 'lon', 'latitud': 'lat'}, inplace=True)

    # Filtrar datos por ruta seleccionada
    if ruta_seleccionada:
        datos = datos[datos['ruta'] == ruta_seleccionada]

    # Calcular el centro promedio de las coordenadas de las rutas seleccionadas
    if not datos.empty:
        center_coordinates = [datos['lat'].mean(), datos['lon'].mean()]
    else:
        # Si no hay datos, establecer un centro predeterminado
        center_coordinates = [39.4699, -0.3763]

    # Configuración del tamaño del mapa
    map_width, map_height = 2300, 1200

    # Crear un mapa de Folium con un estilo simple y gris
    mapa_folium = folium.Map(location=center_coordinates, zoom_start=5, control_scale=True, width=map_width, height=map_height,  tiles='CartoDB positron')

    # Generar colores aleatorios para cada coche_id
    colores = {coche_id: "#{:06x}".format(random.randint(0, 0xFFFFFF)) for coche_id in datos['coche_id'].unique()}

    # Crear diccionario para almacenar polilíneas por coche_id
    polilineas_por_coche = {}

    # Agregar puntos a la ruta con colores diferentes por coche_id
    for _, row in datos.iterrows():
        color = colores[row['coche_id']]
        folium.Marker(location=[row['lat'], row['lon']],
                      popup=f"Coche ID: {row['coche_id']}, Ruta: {row['ruta']}, Coordenadas: ({row['lat']}, {row['lon']})",
                      icon=folium.Icon(color=color)).add_to(mapa_folium)

        # Crear o actualizar polilínea para el coche_id
        if row['coche_id'] not in polilineas_por_coche:
            polilineas_por_coche[row['coche_id']] = []
        polilineas_por_coche[row['coche_id']].append([row['lat'], row['lon']])

    # Agregar polilíneas al mapa
    for coche_id, coordenadas in polilineas_por_coche.items():
        color = colores[coche_id]
        # Evitar que la última coordenada se conecte con la primera
        folium.PolyLine(locations=coordenadas, color=color).add_to(mapa_folium)

    return mapa_folium

if __name__ == "__main__":
    # Nombre de la tabla en BigQuery que quieres leer
    nombre_tabla = tabla_name  # Reemplaza con tu información real

    # Lee los datos de BigQuery
    datos = leer_datos_bigquery(nombre_tabla)

    # Obtener la lista de rutas únicas
    rutas_unicas = datos['ruta'].unique()

    # Agregar un slicer (selectbox) para seleccionar la ruta
    ruta_seleccionada = st.selectbox("Selecciona una ruta:", rutas_unicas)

    # Crea el mapa y la tabla filtrados por la ruta seleccionada
    mapa_folium = crear_mapa_folium(datos, ruta_seleccionada)

     # Muestra el mapa en Streamlit
    with tab1:
        col1, col2  = st.columns([3, 1])
        with col1:
            st.subheader("Rutas en tiempo real")
            folium_static(mapa_folium)
 
    # Muestra la tabla en Streamlit
        with col2:
            st.subheader("Coches")
            st.dataframe(datos[datos['ruta'] == ruta_seleccionada])
            st.subheader("Pasajeros")
            st.dataframe(datos[datos['ruta'] == ruta_seleccionada])
            st.subheader("Pasajeros/coche")
            st.dataframe(datos[datos['ruta'] == ruta_seleccionada])
    
    with tab2:
        col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
        with col1:
            st.subheader("Mapa de calor")
            #Insertar código
        with col2:
            st.subheader("Pasajeros/Coche")
            #Insertar código
            st.subheader("Duración media")
            #Insertar código
        with col3:
            st.subheader("Pasajeros/Día")
            #Insertar código
            st.subheader("Valoración media")
            #Insertar código
        with col4:
            st.subheader("Coches/Día")
            #Insertar código



# Pasajeros totales
    
# Coches totales
    
# Rutas totales
    
# Pasajeros/coche
    
# Pasajeros/día
    
# Coches/día
    
# Duración media [por día gráfica evolutiva]
    
# Valoración media [por día gráfica evolutiva]
    
# Mapa de calor rutas