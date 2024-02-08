import streamlit as st
from streamlit_folium import folium_static
import folium
import json

def read_coordinates_from_json(json_file):
    with open(json_file, 'r') as file:
        data = json.load(file)
    coordinates = [(point['latitud'], point['longitud']) for point in data]
    return coordinates

def main():
      # Título de la aplicación
    st.title("Mapa Interactivo")

    # Ruta al archivo JSON con las coordenadas
    json_file_path = '/Users/adrianacamposnarvaez/Documents/GitHub/DataProject2_BlablaCar/Rutas/Coches/Aeropuerto-Ángel Guimerá_coordinates.json'

    # Leer las coordenadas desde el archivo JSON
    coordinates = read_coordinates_from_json(json_file_path)

    # Coordenadas del centro del mapa
    center_coordinates = coordinates[0]

    # Configuración del tamaño del mapa
    map_width = 2000  # Ancho del mapa en píxeles
    map_height = 1200  # Altura del mapa en píxeles

    # Crear un mapa de Folium
    map = folium.Map(location=center_coordinates, zoom_start=5, control_scale=True, width=map_width, height=map_height)


    # Agregar puntos a la ruta
    for coord in coordinates:
        folium.Marker(location=coord, popup=f"Coordenadas: {coord}").add_to(map)

    # Unir los puntos para formar una ruta
    folium.PolyLine(locations=coordinates, color='blue').add_to(map)

    # Mostrar el mapa en Streamlit
    folium_static(map)

if __name__ == "__main__":
    main()





