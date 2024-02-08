
- Crear entorno 'streamlitenv' a través de command line o Anaconda
- Instalar requirements.txt
- Streamlit run app.py
  
Crear entorno a través de command line: https://docs.streamlit.io/get-started/installation/command-line

Documentación streamlit: https://docs.streamlit.io/get-started/fundamentals/main-concepts

INSERTAR DATOS A BIGQUERY:

- IR AL DIRECTORIO DONDE ESTÁ EL ARCHIVO
- RUN: python datos_bq.py

Para poder introducir los datos es necesario tener creado el dataset y la tabla a través de la interfaz de Google Cloud. Para que los datos se añaden correctamente, es necesario definir el esquema de los datos. Por lo contrario, no se añadirán. 

VISUALIZACIÓN

- Mapa de ubicaciones: marcamos el punto de inicio y final de la ruta 
- ¿Histograma de fecha y hora? A lo mejor no es posible porque la variable datatime no es real
- Estadística descriptiva: media, mediana y desviación estándar de las coordenadas de inicio y fin
- ¿Relación entre coche y usuario? Ej. Cantidad de asignaciones por coche o usuario

ESQUEMA DE NUESTRA TABLA

[
  {"name": "geo", "type": "STRING"},
  {"name": "fin_viaje", "type": "BOOLEAN"},
  {"name": "coches", "type": "RECORD", "mode": "REPEATED", "fields": [
    {"name": "coche_id_message", "type": "STRING"},
    {"name": "coche_id", "type": "INTEGER"},
    {"name": "coche_index_msg", "type": "INTEGER"},
    {"name": "coche_geo", "type": "STRING"},
    {"name": "coche_latitud", "type": "FLOAT64"},
    {"name": "coche_longitud", "type": "FLOAT64"},
    {"name": "coche_datetime", "type": "TIMESTAMP"},
    {"name": "coche_ruta", "type": "STRING"}
  ]},
  {"name": "usuarios", "type": "RECORD", "mode": "REPEATED", "fields": [
    {"name": "user_id_message", "type": "STRING"},
    {"name": "user_id", "type": "INTEGER"},
    {"name": "user_datetime", "type": "TIMESTAMP"},
    {"name": "user_geo", "type": "STRING"},
    {"name": "user_geo_fin", "type": "STRING"},
    {"name": "user_latitud_inicio", "type": "FLOAT64"},
    {"name": "user_longitud_inicio", "type": "FLOAT64"},
    {"name": "user_latitud_destino", "type": "FLOAT64"},
    {"name": "user_longitud_destino", "type": "FLOAT64"}
  ]}
]
