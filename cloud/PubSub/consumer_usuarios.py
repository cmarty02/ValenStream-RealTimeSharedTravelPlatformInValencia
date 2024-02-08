import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.internal.clients import bigquery
import json
from datetime import datetime

#################################################### Adriana ###################################################
project_id = 'woven-justice-411714'
topic_name= 'blablacar_user'
table_name = "woven-justice-411714:ejemplo.usuarios"
suscripcion ='projects/woven-justice-411714/subscriptions/blablacar_user-sub'
#################################################### Cris ######################################################
#project_id = 'dataflow-1-411618'
#topic_name= 'coches'
#table_name = 'dataflow-1-411618:blablacar.rutas'
#suscripcion = 'projects/dataflow-1-411618/subscriptions/coches'


# Recibe datos
def decode_message(msg):
    # Lógica para decodificar el mensaje y cargarlo como JSON
    output = msg.decode('utf-8')
    json_data = json.loads(output)
    print(f"JSON guardado en BigQuery: {json_data}")
    return json_data

# Obtiene la hora actual en formato UTC
current_time_utc = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

class DecodeMessage(beam.DoFn):
    def process(self, element):
        output = element.decode('utf-8')
        json_data = json.loads(output)
        print(f"JSON guardado en BigQuery: {json_data}")
        return [json_data]
    


# Nueva definición del esquema para BigQuery
new_table_schema_personas = bigquery.TableSchema()
new_table_fields_personas = [
    bigquery.TableFieldSchema(name='user_id_message', type='STRING', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='user_id', type='INTEGER', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='user_datetime', type='DATETIME', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='user_geo', type='STRING', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='user_geo_fin', type='STRING', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='user_latitud_inicio', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='user_longitud_inicio', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='user_latitud_destino', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='user_longitud_destino', type='FLOAT', mode='NULLABLE')
]
new_table_schema_personas.fields.extend(new_table_fields_personas)

with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:
    #coches:
    data = (
        p
        | "LeerDesdePubSub2" >> beam.io.ReadFromPubSub(subscription=suscripcion)
        | "decodificar_msg2" >> beam.ParDo(DecodeMessage())
        | "agregar_fecha_hora" >> beam.Map(lambda elem: {**elem, 'user_datetime': current_time_utc})
        | "escribir2" >> beam.io.WriteToBigQuery(
            table=table_name,
            schema=new_table_schema_personas,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
