from google.cloud import bigquery
from datetime import date, timedelta
import random
import string
from faker import Faker


# SUSTITUIMOS EL NOMBRE DEL PROYECTO
client = bigquery.Client("dataflow-1-411618")

# Detalles de la tabla en BigQuery. Sustitumos por el nombre del dataset y la tabla de GCP.
dataset_id = 'blablacar'
table_id = 'usuarios'

fake = Faker()

def generate_unique_user_id(existing_user_ids):
    all_user_ids = set(range(1, 6))
    available_user_ids = list(all_user_ids - set(existing_user_ids))
    
    if not available_user_ids:
        raise ValueError("No hay más user_ids disponibles.")
    
    return random.choice(available_user_ids)

def generate_random_dni():
    return ''.join(random.choices(string.digits, k=8)) + random.choice(string.ascii_uppercase)

existing_user_ids = []
       
num_rows = 5  
data = []
for _ in range(num_rows):
    row = [
        generate_unique_user_id(existing_user_ids),
        fake.first_name(),
        fake.last_name(),
        generate_random_dni(),
        ''.join(random.choices(string.digits, k=9)),
        round(random.uniform(3.0, 5.0), 1),
        random.choice([True, False]),
        date.today() - timedelta(days=random.randint(1, 365))
    ]
    
    data.append(row)
    existing_user_ids.append(row[0])

table_ref = client.dataset(dataset_id).table(table_id)

#Esquema tabla
schema = [
    bigquery.SchemaField("user_id", "INTEGER"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("DNI", "STRING"),
    bigquery.SchemaField("mobile_phone", "STRING"),
    bigquery.SchemaField("rating", "FLOAT64"),
    bigquery.SchemaField("smoking", "BOOLEAN"),
    bigquery.SchemaField("date_register", "DATE"),

]

table = bigquery.Table(table_ref, schema=schema)

try:
    client.create_table(table)  # Intenta crear la tabla
    print(f'Tabla {table_id} creada correctamente.')
except Exception as e:
    print(f'Error al crear la tabla: {e}')

errors = client.insert_rows(table_ref, data, selected_fields=schema)

if errors:
    print(f'Error al insertar datos en BigQuery: {errors}')
else:
    print('Datos insertados correctamente en BigQuery.')
