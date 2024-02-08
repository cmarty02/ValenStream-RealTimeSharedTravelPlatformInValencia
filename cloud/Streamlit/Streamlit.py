import streamlit as st
from google.cloud import bigquery
from folium.plugins import MarkerCluster
from streamlit_folium import folium_static
import time
from folium import Marker
import base64
import random
import folium
from folium.plugins import HeatMap
import pandas as pd
import plotly.express as px
import streamlit as st
import hashlib
import os
from dotenv import load_dotenv

project_id_adri = 'woven-justice-411714'
topic_name_adri = 'blablacar_DataProject2'
tabla_name_coche = 'woven-justice-411714.ejemplo.coches'
tabla_name_usuarios= 'woven-justice-411714.ejemplo.usuarios'
tabla_name = 'dataflow-1-411618.blablacar.asignaciones'
tabla_name_vehiculos = 'dataflow-1-411618.blablacar.vehiculos'
tabla_name_vehiculo_info = 'dataflow-1-411618.blablacar.usuarios'

project_id_adri = 'woven-justice-411714'
topic_name_adri = 'blablacar_DataProject2'
tabla_name_coche = 'woven-justice-411714.ejemplo.coches'
tabla_name_usuarios= 'woven-justice-411714.ejemplo.usuarios'
tabla_name = 'dataflow-1-411618.blablacar.asignaciones'
tabla_rutas_usuarios = 'dataflow-1-411618.blablacar.rutas_usuarios'
tabla_rutas_coches= 'dataflow-1-411618.blablacar.rutas_coches'
tabla_name_vehiculos = 'dataflow-1-411618.blablacar.vehiculos'
tabla_name_vehiculo_info = 'dataflow-1-411618.blablacar.usuarios'



################################################### Configuramos la página para que ocupe la anchura completa del navegador ################################

st.set_page_config(layout="wide", page_title="¡Bienvenido/a a Streamlit!", page_icon="🚗",menu_items={
        'Get Help': 'https://www.extremelycoolapp.com/help',
        'Report a bug': "https://www.extremelycoolapp.com/bug",
        'About': "# This is a header. This is an *extremely* cool app!"
    })


# Cargar variables de entorno desde un archivo .env
load_dotenv()

# Obtener la contraseña almacenada en la variable de entorno
contrasena_guardada = os.getenv("CONTRASENA_HASH")

# Solicitar al usuario que ingrese la contraseña
contrasena_ingresada = st.text_input("Por favor, ingresa la contraseña para acceder a las pestañas:", type="password")

# Hash de la contraseña ingresada
hash_ingresado = hashlib.sha256(contrasena_ingresada.encode()).hexdigest()

# Verificar si el hash de la contraseña ingresada es igual al hash almacenado
if hash_ingresado == contrasena_guardada:
    
    logo_url="https://user-images.githubusercontent.com/8149019/166203710-737d477f-c325-4417-8518-7b378918d1f1.png"
    st.image(logo_url, width=40)
    st.write("Bienvenido al dashboard de BlaBlaCar para la ciudad de Valencia. Aquí podrás visualizar y analizar datos relacionados con viajes compartidos en la ciudad.")
    # Creamos dos pestañas para las distintas visualizaciones que necesitamos
    tab1, tab2, tab3 , tab4 = st.tabs(["Asignaciones", "Origen datos", "Datos", "Gráficos"])

    ################################################################################################################################################################
    client = bigquery.Client()
    

    ################################################################################################################################################################
    ################################################################ Funciones SQL #################################################################################
    ################################################################################################################################################################

    def leer_datos_bigquery_user(tabla):
            # Agrega comillas inversas alrededor del nombre de la tabla
            query = f"SELECT * FROM {tabla}"  
            return client.query(query).to_dataframe()
        
    def leer_datos_bigquery_filtrado_fin(tabla):
        # Agrega comillas inversas alrededor del nombre de la tabla
        query = f"""
            SELECT coche_id, MAX(coche_datetime) AS coche_datetime, user_id, fin_viaje 
            FROM {tabla} 
            WHERE fin_viaje = True 
            GROUP BY coche_id, user_id, fin_viaje 
            ORDER BY user_id ASC
        """
        return client.query(query).to_dataframe()
    
    def leer_datos_bigquery(tabla):
        # Agrega comillas inversas alrededor del nombre de la tabla
        query = f"SELECT * FROM {tabla} ORDER BY coche_index_msg ASC "  
        return client.query(query).to_dataframe()

    def leer_datos_bigquery_filtrado(tabla):
        # Agrega comillas inversas alrededor del nombre de la tabla
        query = f"SELECT coche_id,coche_datetime,user_id,inicio_viaje FROM {tabla} ORDER BY coche_datetime ASC "  
        return client.query(query).to_dataframe()


    def leer_datos_bigquery_coche(tabla_coche):
        # Agrega comillas inversas alrededor del nombre de la tabla
        query = f"SELECT * FROM {tabla_coche}"  
        return client.query(query).to_dataframe()

    def coches_totales(tabla_coches_totales):
        query = f"SELECT COUNT(DISTINCT coche_id) as coches_totales FROM `{tabla_coches_totales}`"
        return client.query(query).to_dataframe()

    def coches_dia(tabla_coches_dia):
        query = f"SELECT DATE(coche_datetime) as fecha, COUNT(DISTINCT coche_id) as coches_dia FROM `{tabla_coches_dia}` GROUP BY fecha"
        return client.query(query).to_dataframe()

    def total_viajes(tabla):
        # Agrega comillas inversas alrededor del nombre de la tabla
        query = f"""
            SELECT COUNT(*) AS total_viajes
            FROM {tabla} 
            WHERE inicio_viaje = True
        """
        return client.query(query).to_dataframe().iloc[0, 0]
    
    def coches_total_viaje(tabla):
        query = f"SELECT coche_id, COUNT(user_id) as total_viajes FROM `{tabla}` GROUP BY coche_id;"
        return client.query(query).to_dataframe()



    def join_asignacion_vehiculo(tabla_asignaciones, tabla_vehiculos):
        query = f"""
        SELECT
            coches.coche_id,
            COUNT(asignaciones.user_id) as total_viajes,
            coches.license_plate,
            coches.owner_name,
            coches.car_brand,
            coches.car_model
        FROM `{tabla_vehiculos}` as coches
        LEFT JOIN `{tabla_asignaciones}` as asignaciones
        ON coches.coche_id = asignaciones.coche_id
        GROUP BY coches.coche_id, coches.car_brand, coches.owner_name, coches.car_model, coches.license_plate
        """
        return client.query(query).to_dataframe()

    def calcular_precio(tabla_asignaciones):
        query = f"""
        SELECT
        asignaciones.user_id,
        COUNT(asignaciones.coche_id) as total_viajes,
        SUM(CASE WHEN asignaciones.inicio_viaje THEN asignaciones.coche_index_msg ELSE 0 END) as sum_inicio,
        SUM(CASE WHEN asignaciones.fin_viaje THEN asignaciones.coche_index_msg ELSE 0 END) as sum_fin,
        SUM(CASE WHEN asignaciones.fin_viaje THEN asignaciones.coche_index_msg ELSE 0 END) - 
        SUM(CASE WHEN asignaciones.inicio_viaje THEN asignaciones.coche_index_msg ELSE 0 END) as km_distancia_recorrida,
        ROUND((SUM(CASE WHEN asignaciones.fin_viaje THEN asignaciones.coche_index_msg ELSE 0 END) - 
        SUM(CASE WHEN asignaciones.inicio_viaje THEN asignaciones.coche_index_msg ELSE 0 END)) * 0.35, 1) as importe_euros
        FROM `{tabla_asignaciones}` as asignaciones
        GROUP BY asignaciones.user_id
        order by importe_euros;
        """
        return client.query(query).to_dataframe()


    # Función para crear un mapa de Folium con la ruta y colores diferentes por coche_id
    def crear_mapa_folium(datos, ruta_seleccionada=None):
        datos.rename(columns={'coche_longitud': 'lon', 'coche_latitud': 'lat'}, inplace=True)

        # Filtrar datos por ruta seleccionada
        if ruta_seleccionada:
            datos = datos[datos['coche_ruta'] == ruta_seleccionada]

        # Calcular el centro promedio de las coordenadas de las rutas seleccionadas
        if not datos.empty:
            center_coordinates = [datos['lat'].mean(), datos['lon'].mean()]
        else:
            # Si no hay datos, establecer un centro predeterminado
            center_coordinates = [39.4699, -0.3763]

        # Configuración del tamaño del mapa
        map_width, map_height = 2000, 1200

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
                        popup=f"Coche ID: {row['coche_id']}, Ruta: {row['coche_ruta']}, Coordenadas: ({row['lat']}, {row['lon']})",
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


    ################################################################################################

    with tab3:

        st.write("Información adicional/personal de los vehiculos:")
        datos_vehiculos = leer_datos_bigquery_coche(tabla_name_vehiculos)
        st.dataframe(datos_vehiculos)
        st.write("Información adicional/personal de los usuarios:") 
        datos_usu = leer_datos_bigquery_coche(tabla_name_vehiculo_info)
        st.dataframe(datos_usu)


    ##########
        datos_vehiculos = coches_totales(tabla_name_vehiculos)
        # Extrae el valor que deseas mostrar en el KPI
        total_coches = datos_vehiculos['coches_totales'].values[0]
        # Muestra el KPI
        st.metric("Total de Vehículos", total_coches)
    #############
        
        st.write("Información coches por día:")
        datos_vehiculos_dia = coches_dia(tabla_name)
        st.dataframe(datos_vehiculos_dia)

        st.write("Usuarios por coche:")
        datos_user_coche = coches_total_viaje(tabla_name)
        st.dataframe(datos_user_coche)

        st.write("Usuarios por coche:")
        datos_join = join_asignacion_vehiculo(tabla_name,tabla_name_vehiculos)
        st.dataframe(datos_join)



        color_texto = "#6495ED"  # Puedes ajustar este código de color según tus preferencias
        #color_texto = "#6495ED" 
        # Crea el código HTML y CSS para cambiar el color del texto
        html_code = f"""
        <style>
            p {{
                color: {color_texto};
            }}
        </style>
        """
        # Aplica el estilo utilizando st.markdown
        st.markdown(html_code, unsafe_allow_html=True)
        st.write("Precio del usuario:")
        datos_join = calcular_precio(tabla_name)
        st.dataframe(datos_join)



    ################################################################################################


    with tab2:

        datos = leer_datos_bigquery_coche(tabla_name_coche)
        rutas_unicas = datos['coche_id'].unique()
        # Agregar un slicer (selectbox) para seleccionar la ruta
        ruta_seleccionada = st.selectbox("Selecciona una ruta:", rutas_unicas)
        # Crea el mapa y la tabla filtrados por la ruta seleccionada
        mapa_folium = crear_mapa_folium(datos, ruta_seleccionada)
        # Muestra la tabla en Streamlit
        st.write("Datos de Rutas coches")
        st.dataframe(datos[datos['coche_id'] == ruta_seleccionada])
        #st.dataframe(datos)




    with tab4:

        col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
        with col1:
                st.subheader("Mapa de calor")
                # Configuración de BigQuery
                project_id = 'woven-justice-411714'
                dataset_id = 'ejemplo'
                table_id = 'asignaciones'
                
                datos = leer_datos_bigquery(tabla_name)
                if not datos.empty:
                    center_coordinates = [datos['coche_latitud'].mean(), datos['coche_longitud'].mean()]
                else:
                    center_coordinates = [0, 0]

                mymap = folium.Map(location=center_coordinates, zoom_start=13)

                # Agrega el mapa de calor con las coordenadas de latitud y longitud
                heat_data = [[row['coche_latitud'], row['coche_longitud']] for index, row in datos.iterrows()]
                HeatMap(heat_data).add_to(mymap)

                # Muestra el mapa en Streamlit
                folium_static(mymap)
                

        with col2:
            st.subheader("Grafico de viajes asigandos")
                # Lee los datos de BigQuery
                # Lee los datos de BigQuery
            datos = leer_datos_bigquery(tabla_name)
                # Filtra los datos para aquellos con inicio_viaje=True y fin_viaje=True
            viajes_completos = datos[(datos['inicio_viaje'] == True) ]
                # Cuenta el número de registros para cada coche
            conteo_viajes_completos = viajes_completos['coche_id'].value_counts()
                # Crea un gráfico de barras con Plotly Express
            fig = px.bar(x=conteo_viajes_completos.index, y=conteo_viajes_completos.values, labels={'x': 'ID del Coche', 'y': 'Número de Viajes Completos'},title='Número de Viajes Completos por Coche')
            # Muestra el gráfico en Streamlit
            st.plotly_chart(fig)




    ################################################################################################
        

        with tab1:
            # Configura tus detalles de proyecto, conjunto de datos y tabla
            project_id = 'dataflow-1-411618'
            dataset_id = 'blablacar'
            table_id = 'asignaciones'

            # Recupera los datos de BigQuery
            df_filtrado = leer_datos_bigquery_filtrado(tabla_name)
            df_filtrado_fin = leer_datos_bigquery_filtrado_fin(tabla_name)
            df_filtrado_total_viajes = total_viajes(tabla_name)
            
            # Muestra los DataFrames uno al lado del otro
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(label="Total de Viajes Completados", value=df_filtrado_total_viajes, delta=None)
                
            with col2:
                st.write("Ultimos Viajes Iniciados")
                st.write(df_filtrado)
                
            with col3:
                st.write("Ultimos Viajes Finalizados")
                st.write(df_filtrado_fin)

            # Recupera los datos de BigQuery
            df_usuarios = leer_datos_bigquery_user(tabla_rutas_usuarios)
            df_coches =  leer_datos_bigquery(tabla_rutas_coches)
            st.write("Mapa interactivo")
        
            # Establece las coordenadas del centro de Valencia, España
            valencia_center_coordinates = [39.44292, -0.36584]
        
            # Contenedor para el mapa
            map_container = st.empty()
    

    # Crear el mapa inicial
    mymap = folium.Map(location=valencia_center_coordinates, zoom_start=13, tiles='cartodbpositron')

    # Diccionario para almacenar los marcadores de coches
    car_markers = {}

    # Generadores para emitir las coordenadas de cada coche en el orden especificado
    car_coordinates_generators = {}

    # Inicializar los generadores para cada coche
    for car_id in df_coches['coche_id'].unique():
        def get_car_coordinates(car_id):
            for idx in df_coches[df_coches['coche_id'] == car_id]['coche_index_msg']:
                car_latitud = float(df_coches[(df_coches['coche_id'] == car_id) & (df_coches['coche_index_msg'] == idx)]['coche_latitud'].iloc[0])
                car_longitud = float(df_coches[(df_coches['coche_id'] == car_id) & (df_coches['coche_index_msg'] == idx)]['coche_longitud'].iloc[0])
                yield car_latitud, car_longitud
        car_coordinates_generators[car_id] = get_car_coordinates(car_id)

    def reset_generator(generator):
        while True:
            for value in generator:
                yield value
                
                
    # Bucle principal para actualizar los marcadores de coches
    while True:
        # Limpiar el mapa antes de agregar nuevos marcadores
        mymap = folium.Map(location=valencia_center_coordinates, zoom_start=13, tiles='cartodbpositron')

        # Agregar marcadores de usuarios al mapa
        for _, user_row in df_usuarios.iterrows():
            user_latitud = float(user_row['user_latitud_inicio'])
            user_longitud = float(user_row['user_longitud_inicio'])  
            
            icon_user = folium.Icon(color='blue', icon='user', prefix='fa')
            marker_user = folium.Marker(location=[user_latitud, user_longitud], popup=f"User ID: {user_row['user_id']}, User Latitud: {user_latitud}, User Longitud: {user_longitud}", icon=icon_user).add_to(mymap)

        # Obtener las coordenadas de cada coche y actualizar los marcadores en el mapa
        for car_id, generator in car_coordinates_generators.items():
            generator = reset_generator(generator)
            car_latitud, car_longitud = next(generator)

            # Actualizar marcador de coche en el mapa
            icon_car = folium.Icon(color='red', icon='car', prefix='fa')
            marker_car = folium.Marker(location=[car_latitud, car_longitud], popup=f"Vehicle ID: {car_id}, Car Latitud: {car_latitud}, Car Longitud: {car_longitud}, User Latitud: {user_latitud}, User Longitud: {user_longitud}", icon=icon_car)
            marker_car.add_to(mymap)

        # Convierte el mapa de Folium a HTML y muestra el HTML directamente en Streamlit
        map_html = f'<div style="width: 100%;"><iframe width="100%" height="500" src="data:text/html;base64,{base64.b64encode(mymap._repr_html_().encode()).decode()}" frameborder="0" allowfullscreen="true"></iframe></div>'
        map_container.markdown(map_html, unsafe_allow_html=True)
            
        # Esperar 2 segundos antes de la próxima iteración
        time.sleep(2)

               
else:
    st.write("Contraseña incorrecta. Por favor, ingresa la contraseña de nuevo para acceder a las pestañas.")