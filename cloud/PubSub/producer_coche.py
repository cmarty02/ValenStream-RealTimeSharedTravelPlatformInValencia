import os
import json
import xml.etree.ElementTree as ET
from google.cloud import pubsub_v1
import time
from datetime import datetime
import uuid
import pandas as pd


#################################################### Adriana ###################################################
project_id = 'woven-justice-411714'
topic_name  = 'blablacar_car'

##############################################Cris################################################################# 
# Configuración de proyectos y temas
#project_id = 'dataflow-1-411618'
#topic_name= 'coches_stream'
###############################################################################################################


# Clase para la publicación en Pub/Sub
class PubSubProducer:
    def __init__(self, project_id, topic_name):
        self.project_id = project_id
        self.topic_name = topic_name
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_name)

    def publish_message(self, message):
        message['coche_datetime'] = datetime.utcnow().isoformat()
        message['coche_id_message'] = str(uuid.uuid4())

        data = json.dumps(message).encode("utf-8")
        future = self.publisher.publish(self.topic_path, data)
        print(f"Coche_Publicado en Pub/Sub: {message}")
        return future

# Función para cargar coordenadas desde un archivo KML
def cargar_coordenadas_desde_kml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    coordinates = []

    for coordinates_tag in root.findall('.//{http://www.opengis.net/kml/2.2}coordinates'):
        coordinates_text = coordinates_tag.text.strip()

        for coord_set in coordinates_text.split('\n'):
            coordinates.append(coord_set)

    return coordinates

# Función para convertir coordenadas a formato JSON con campos adicionales
def convertir_a_json(coordinates, coche_id, ruta_nombre):
    coordinates_json = []
    for index, coord_text in enumerate(coordinates, start=1):
        lat, lon, alt = [float(coord) for coord in coord_text.split(',')]
        coordinates_json.append({
            'coche_id_message': None,
            'coche_id': coche_id,
            'coche_index_msg': index,
            'coche_geo': f"{lon}{lat}",
            'coche_latitud': lon,
            'coche_longitud': lat,
            'coche_datetime': None,
            'coche_ruta': ruta_nombre
        })

    return coordinates_json

# Clase modificada para incluir la funcionalidad de ordenar y enviar mensajes
class PubSubProducerModified(PubSubProducer):
    def publish_sorted_messages(self, df):
        for _, coord_message in df.iterrows():
            self.publish_message(coord_message.to_dict())
            time.sleep(5)

def main():
    # Directorio que contiene los archivos KML
    directory_path = './rutas/coches/'

    # Obtener la lista de archivos KML en el directorio
    kml_files = [f for f in os.listdir(directory_path) if f.endswith('.kml')]

    # Verificar si hay archivos KML en el directorio
    if not kml_files:
        print("No se encontraron archivos KML en el directorio.")
        return

    # Inicializar una lista para almacenar los datos de coordenadas
    all_coordinates = []

    # Inicializar el contador de coches
    coche_id_counter = 1

    # Iterar sobre cada archivo KML
    for kml_file in kml_files:
        file_path = os.path.join(directory_path, kml_file)

        # Cargar coordenadas desde el archivo KML
        coordinates = cargar_coordenadas_desde_kml(file_path)

        # Convertir a formato JSON con campos adicionales
        coordinates_json = convertir_a_json(coordinates, coche_id_counter, kml_file)

        # Incrementar el contador de coches para el próximo archivo
        coche_id_counter += 1

        # Agregar las coordenadas al conjunto de todas las coordenadas
        all_coordinates.extend(coordinates_json)

    # Crear un DataFrame a partir de todas las coordenadas
    df = pd.DataFrame(all_coordinates)

    # Ordenar el DataFrame por el campo 'coche_index_msg'
    df = df.sort_values(by='coche_index_msg')

    # Crear una instancia de la clase PubSubProducerModified
    pubsub_producer = PubSubProducerModified(project_id=project_id, topic_name=topic_name)

    # Enviar coordenadas ordenadas a través de Pub/Sub
    pubsub_producer.publish_sorted_messages(df)

    print("Todos los archivos KML han sido procesados y los mensajes han sido enviados a Pub/Sub.")

if __name__ == "__main__":
    main()
