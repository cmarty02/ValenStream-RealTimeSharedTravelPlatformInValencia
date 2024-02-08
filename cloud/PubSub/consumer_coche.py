
################################ Script para escribir en Big Query la información de los coches ####################################

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.internal.clients import bigquery
import json
from datetime import datetime

#################################################### Adriana ###################################################
project_id = 'woven-justice-411714'
topic_name= 'blablacar_car'
table_name = "woven-justice-411714:ejemplo.coches"
suscripcion ='projects/woven-justice-411714/subscriptions/blablacar_car-sub'

#################################################### Cris ######################################################
#project_id = 'dataflow-1-411618'
#topic_name= 'coches'
#table_name = 'dataflow-1-411618:blablacar.rutas'
#suscripcion = 'projects/dataflow-1-411618/subscriptions/coches'

################################################# Jesus ######################################################
#project_id = 'blablacar-412022'
#topic_name= 'coches'
#table_name = 'blablacar-412022.dataset.coches'
#suscripcion = 'projects/blablacar-412022/subscriptions/coches-sub'


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
new_table_schema = bigquery.TableSchema()
new_table_fields = [
    bigquery.TableFieldSchema(name='coche_id_message', type='STRING', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_id', type='INTEGER', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_index_msg', type='INTEGER', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_geo', type='STRING', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_latitud', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_longitud', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_datetime', type='datetime', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_ruta', type='STRING', mode='NULLABLE')
]
new_table_schema.fields.extend(new_table_fields)




with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:
    #coches:
    data = (
        p
        | "LeerDesdePubSub" >> beam.io.ReadFromPubSub(subscription=suscripcion)
        | "decodificar_msg" >> beam.ParDo(DecodeMessage())
        | "agregar_fecha_hora" >> beam.Map(lambda elem: {**elem, 'coche_datetime': current_time_utc})
        | "escribir" >> beam.io.WriteToBigQuery(
            table= table_name,
            schema=new_table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )

