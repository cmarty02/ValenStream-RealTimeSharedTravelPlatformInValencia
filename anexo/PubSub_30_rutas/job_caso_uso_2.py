import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.internal.clients import bigquery
import json
from datetime import datetime
from apache_beam.transforms import CoGroupByKey
from apache_beam.options.pipeline_options import GoogleCloudOptions


################config################################

#
suscripcion_coche = 'projects/woven-justice-411714/subscriptions/blablacar_car_30-sub'
suscripcion_usuario = 'projects/woven-justice-411714/subscriptions/blablacar_user_30-sub'
project_id = 'woven-justice-411714'
bucket_name = 'woven-justice-411714'




#########funciones#######################################

# Recibe datos
class DecodeMessage(beam.DoFn):
    def process(self, element):
        output = element.decode('utf-8')
        json_data = json.loads(output)
        return [json_data]


# Función para extraer la clave 'user_geo' de cada elemento para el inicio del viaje
def extract_geo_user(element):
    geo = element.get('user_geo', None)
    return (geo, element)

# Función para extraer la clave 'user_geo_fin' de cada elemento para el fin del viaje
def extract_geo_fin(element):
    geo = element.get('user_geo_fin', None)
    return (geo, element)

# Función para extraer la clave 'coche_geo' de cada elemento
def extract_geo_coche(element):
    geo = element.get('coche_geo', None)
    return (geo, element)

# Función para filtrar casos coincidentes y no coincidentes para el inicio del viaje
class FilterCoincidentCases_inicio(beam.DoFn):
    def process(self, element):
        geo_key, messages = element
        coches = messages['coches']
        usuarios = messages['usuarios']

        if coches and usuarios:
            yield {'geo': geo_key, 'coches': coches, 'usuarios': usuarios, 'inicio_viaje': True}
        else:
            yield {'geo': geo_key, 'coches': coches, 'usuarios': usuarios, 'inicio_viaje': False}

# Función para filtrar casos coincidentes y no coincidentes para el fin del viaje
class FilterCoincidentCases_fin(beam.DoFn):
    def process(self, element):
        geo_key, messages = element
        coches = messages['coches']
        usuarios = messages['usuarios']

        if coches and usuarios:
            yield {'geo': geo_key, 'coches': coches, 'usuarios': usuarios, 'fin_viaje': True}
        else:
            yield {'geo': geo_key, 'coches': coches, 'usuarios': usuarios, 'fin_viaje': False}
            
##########fucncion_process_data_INICIO############

class BuildRowFn(beam.DoFn):
    def process(self, element):
        row = {}
        geo = element['geo']
        coche = element['coches'][0]
        user = element['usuarios'][0][0]

        row['geo'] = geo
        row['coche_id'] = coche['coche_id']
        row['coche_index_msg'] = coche['coche_index_msg']
        row['coche_geo'] = coche['coche_geo']
        row['coche_latitud'] = coche['coche_latitud']
        row['coche_longitud'] = coche['coche_longitud']
        row['coche_datetime'] = coche['coche_datetime']
        row['coche_ruta'] = coche['coche_ruta']

        row['user_id'] = user['user_id']
        row['user_datetime'] = user['user_datetime']
        row['user_geo'] = user['user_geo']
        row['user_geo_fin'] = user['user_geo_fin']
        row['user_latitud_inicio'] = user['user_latitud_inicio']
        row['user_longitud_inicio'] = user['user_longitud_inicio']
        row['user_latitud_destino'] = user['user_latitud_destino']
        row['user_longitud_destino'] = user['user_longitud_destino']

        row['inicio_viaje'] = element['inicio_viaje']
        

        return [row]

##########fucncion_process_data_FIN############

class BuildRowFn_fin(beam.DoFn):
    def process(self, element):
        row = {}
        geo = element['geo']
        coche = element['coches'][0]
        user = element['usuarios'][0][0]

        row['geo'] = geo
        row['coche_id'] = coche['coche_id']
        row['coche_index_msg'] = coche['coche_index_msg']
        row['coche_geo'] = coche['coche_geo']
        row['coche_latitud'] = coche['coche_latitud']
        row['coche_longitud'] = coche['coche_longitud']
        row['coche_datetime'] = coche['coche_datetime']
        row['coche_ruta'] = coche['coche_ruta']

        row['user_id'] = user['user_id']
        row['user_datetime'] = user['user_datetime']
        row['user_geo'] = user['user_geo']
        row['user_geo_fin'] = user['user_geo_fin']
        row['user_latitud_inicio'] = user['user_latitud_inicio']
        row['user_longitud_inicio'] = user['user_longitud_inicio']
        row['user_latitud_destino'] = user['user_latitud_destino']
        row['user_longitud_destino'] = user['user_longitud_destino']

        row['fin_viaje'] = element['fin_viaje']
        

        return [row]
    

##########pipeline#################################

# Crear el pipeline

with beam.Pipeline(options=PipelineOptions(
        streaming=True,
        # save_main_session=True
        job_name = "edem-dataflow",
        project=project_id,
        runner="DataflowRunner",
        #donde guarda los archivos
        temp_location=f"gs://{bucket_name}/tmp",
        staging_location=f"gs://{bucket_name}/staging",
        region="europe-west4"
    )) as p: 

    # Coches
    coches_data = (
        p
        | "Coche_LeerDesdePubSub" >> beam.io.ReadFromPubSub(subscription='projects/woven-justice-411714/subscriptions/blablacar_car_30-sub')
        | "Coche_decodificar_msg" >> beam.ParDo(DecodeMessage())
        | "Coche_Extraer_Clave_geo" >> beam.Map(extract_geo_coche)
        | "Coche_ventana_5_minutos" >> beam.WindowInto(beam.window.FixedWindows(600))
    )

    # Usuarios
    usuarios_data = (
        p
        | "Usuario_LeerDesdePubSub" >> beam.io.ReadFromPubSub(subscription='projects/woven-justice-411714/subscriptions/blablacar_user_30-sub')
        | "Usuario_decodificar_msg" >> beam.ParDo(DecodeMessage())
        | "Usuario_ventana_5_minutos" >> beam.WindowInto(beam.window.FixedWindows(600))
    )

    # Derivar dos flujos distintos para inicio y fin del viaje
    usuarios_data_inicio = (
        usuarios_data
        | "Usuario_Extraer_Clave_geo_inicio" >> beam.Map(extract_geo_user)
        | "Etiquetar_inicio_viaje" >> beam.Map(lambda x: (x[0], (x[1], 'inicio')))
    )

    usuarios_data_fin = (
        usuarios_data
        | "Usuario_Extraer_Clave_geo_fin" >> beam.Map(extract_geo_fin)
        | "Etiquetar_fin_viaje" >> beam.Map(lambda x: (x[0], (x[1], 'fin')))
    )

    # Realizar un CoGroupByKey en base al campo 'geo'_inicio
    joined_data_inicio = (
        {'coches': coches_data, 'usuarios': usuarios_data_inicio}
        | "Merge_Mensajes_por_geo" >> CoGroupByKey()
        | "Filtrar_Casos_Coincidentes" >> beam.ParDo(FilterCoincidentCases_inicio())
        | "Filtrar_Solo_Coincidentes" >> beam.Filter(lambda element: element['inicio_viaje'])
        | "TransformToBigQueryFormat_ini" >> beam.ParDo(BuildRowFn())
        | "WriteToBigQuery_ini" >> beam.io.WriteToBigQuery(
            table="woven-justice-411714.ejemplo.asignaciones",
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
    
        # Realizar un CoGroupByKey en base al campo 'geo'_fin
    joined_data_fin = (
        {'coches': coches_data, 'usuarios': usuarios_data_fin}
        | "Merge_Mensajes_por_geo_fin" >> CoGroupByKey()
        | "Filtrar_Casos_Coincidentes_fin" >> beam.ParDo(FilterCoincidentCases_fin())
        | "Filtrar_Solo_Coincidentes_fin" >> beam.Filter(lambda element: element['fin_viaje'])
        | "TransformToBigQueryFormat_fin" >> beam.ParDo(BuildRowFn_fin())
        | "WriteToBigQuery_fin" >> beam.io.WriteToBigQuery(
            table="woven-justice-411714.ejemplo.asignaciones",
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
