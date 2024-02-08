import os
import json
import xml.etree.ElementTree as ET
from google.cloud import pubsub_v1
import time
from datetime import datetime
import uuid

#################################################### Adriana ###################################################
project_id = 'woven-justice-411714'
topic_name = 'blablacar_car'
#################################################### Cris ######################################################
#project_id = 'dataflow-1-411618'
#topic_name= 'coches_stream'
###################################################   Jesús   ###################################################
#project_id = 'blablacar-412022'
#topic_name = 'coches'
###############################################################################################################



# Clase para la publicación en Pub/Sub
class PubSubProducer:
    def __init__(self, project_id, topic_name):
        self.project_id = project_id
        self.topic_name = topic_name
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_name)

    def publish_message(self, message):
        # Agregar el campo 'datetime' al mensaje JSON
        message['coche_datetime'] = datetime.utcnow().isoformat() 
        # Agregar el campo 'id_message' como un identificador único alfanumérico
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


######### como hay muchas coordenadas y en Streamlit no queda bien vamos a envair las coordenadas pares 

def main():
    # Directorio que contiene los archivos KML
    directory_path = './rutas/coches1/'

    # Obtener la lista de archivos KML en el directorio
    kml_files = [f for f in os.listdir(directory_path) if f.endswith('.kml')]

    # Verificar si hay archivos KML en el directorio
    if not kml_files:
        print("No se encontraron archivos KML en el directorio.")
        return

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

        # Crear una instancia de la clase PubSubProducer
        pubsub_producer = PubSubProducer(project_id=project_id, topic_name=topic_name)

        # Enviar coordenadas a través de Pub/Sub, seleccionando cada segunda coordenada
        for index, coord_message in enumerate(coordinates_json):
            pubsub_producer.publish_message(coord_message)
            time.sleep(3)  # Esperar 1 segundo entre mensajes

        print(f"Coordenadas alternas de {kml_file} han sido enviadas a Pub/Sub con ID de coche {coche_id_counter - 1}.")

    print("Todos los archivos KML han sido procesados y los mensajes alternos han sido enviados a Pub/Sub.")

if __name__ == "__main__":
    main()