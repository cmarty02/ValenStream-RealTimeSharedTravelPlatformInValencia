import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import CoGroupByKey
import json

# Definir suscripciones y otros detalles si es necesario
suscripcion_coche = 'projects/dataflow-1-411618/subscriptions/coches_stream-sub'
suscripcion_usuario = 'projects/dataflow-1-411618/subscriptions/usuarios_stream-sub'

# Recibe datos
class DecodeMessage(beam.DoFn):
    def process(self, element):
        output = element.decode('utf-8')
        json_data = json.loads(output)
        return [json_data]

# Función para extraer la clave 'geo' de cada elemento
def extract_geo(element):
    geo = element.get('geo', None)
    print("Geo Extraída:", geo)
    return (geo, element)


# Función para filtrar casos coincidentes
class FilterCoincidentCases(beam.DoFn):
    def process(self, element):
        geo_key, messages = element
        coches = [coche for coche_list in messages['coches'] for coche in coche_list]
        usuarios = [usuario for usuario_list in messages['usuarios'] for usuario in usuario_list]

        if coches and usuarios:
            # Emitir solo si la clave 'geo' es única
            yield {'geo': geo_key, 'coches': coches, 'usuarios': usuarios}



# Crear el pipeline
with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:
    # Coches
    coches_data = (
        p
        | "Coche_LeerDesdePubSub" >> beam.io.ReadFromPubSub(subscription=suscripcion_coche)
        | "Coche_decodificar_msg" >> beam.ParDo(DecodeMessage())
        | "Coche_Extraer_Clave_geo" >> beam.Map(extract_geo)
        | "Coche_ventana_5_minutos" >> beam.WindowInto(beam.window.FixedWindows(15))
    )

    # Usuarios
    usuarios_data = (
        p
        | "Usuario_LeerDesdePubSub" >> beam.io.ReadFromPubSub(subscription=suscripcion_usuario)
        | "Usuario_decodificar_msg" >> beam.ParDo(DecodeMessage())
        | "Usuario_Extraer_Clave_geo" >> beam.Map(extract_geo)
        | "Usuario_ventana_5_minutos" >> beam.WindowInto(beam.window.FixedWindows(15))
    )

    # Realizar un CoGroupByKey en base al campo 'geo'
    joined_data = (
        {'coches': coches_data, 'usuarios': usuarios_data}
        | "Merge_Mensajes_por_geo" >> CoGroupByKey()
        | "Filtrar_Casos_Coincidentes" >> beam.ParDo(FilterCoincidentCases())
        | "Imprimir_Resultados" >> beam.Map(print)
    )