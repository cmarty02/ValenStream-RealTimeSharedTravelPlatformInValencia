import os
import json
import xml.etree.ElementTree as ET
from google.cloud import pubsub_v1
import time
from datetime import datetime
import uuid

#################################################### Adriana ###################################################
project_id = 'woven-justice-411714'
topic_name  = 'blablacar_user'
#################################################### Cris ######################################################
#project_id = 'dataflow-1-411618'
#topic_name= 'usuarios_stream'
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
        message['user_datetime'] = datetime.utcnow().isoformat() 
        # Agregar el campo 'id_message' como un identificador único alfanumérico
        message['user_id_message'] = str(uuid.uuid4())
        
        data = json.dumps(message).encode("utf-8")
        future = self.publisher.publish(self.topic_path, data)
        print(f"Usuario_Publicado en Pub/Sub: {message}")
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
def convertir_a_json(coordinates, user_id, ruta_nombre):
    # Tomar solo la primera y la última coordenada
    first_coord_text = coordinates[0]
    last_coord_text = coordinates[-1]

    # Convertir las coordenadas a formato JSON con campos adicionales
    first_lat, first_lon, first_alt = [float(coord) for coord in first_coord_text.split(',')]
    last_lat, last_lon, last_alt = [float(coord) for coord in last_coord_text.split(',')]

    json_data = {
        'user_id_message': None,
        'user_id': user_id,
        'user_datetime': None,
        'user_geo': f"{first_lon}{first_lat}",
        'user_geo_fin': f"{last_lon}{last_lat}",
        'user_latitud_inicio': first_lon,
        'user_longitud_inicio': first_lat,
        'user_latitud_destino': last_lon,
        'user_longitud_destino': last_lat
    }

    return json_data

# Función principal
def main():

    # Directorio que contiene los archivos KML
    directory_path = './rutas/personas/'

    # Obtener la lista de archivos KML en el directorio
    kml_files = [f for f in os.listdir(directory_path) if f.endswith('.kml')]

    # Verificar si hay archivos KML en el directorio
    if not kml_files:
        print("No se encontraron archivos KML en el directorio.")
        return

    # Inicializar el id_usuarios
    user_id_counter = 9990

    # Iterar sobre cada archivo KML
    for kml_file in kml_files:
        file_path = os.path.join(directory_path, kml_file)

        # Cargar coordenadas desde el archivo KML
        coordinates = cargar_coordenadas_desde_kml(file_path)

        # Convertir a formato JSON con campos adicionales
        coordinates_json = convertir_a_json(coordinates, user_id_counter, kml_file)

        # Incrementar el contador de coches para el próximo archivo
        user_id_counter += 1

        # Crear una instancia de la clase PubSubProducer
        pubsub_producer = PubSubProducer(project_id=project_id, topic_name=topic_name)

        # Enviar el JSON a través de Pub/Sub
        pubsub_producer.publish_message(coordinates_json)
        time.sleep(30)  # Esperar 30 segundos antes de procesar el siguiente archivo

        print(f"JSON con la primera y última geolocalización de {kml_file} enviado a Pub/Sub.")

    print("Todos los archivos KML han sido procesados y los JSON han sido enviados a Pub/Sub.")

if __name__ == "__main__":
    main()