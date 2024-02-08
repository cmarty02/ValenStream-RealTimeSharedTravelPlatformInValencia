from faker import Faker
from faker_vehicle import VehicleProvider
from google.cloud import bigquery
from faker.providers import color
from faker.providers import automotive
import random
import string


fake = Faker()
fake.add_provider(automotive)  
fake.add_provider(color)
fake.add_provider(VehicleProvider)


# SUSTITUIMOS EL NOMBRE DEL PROYECTO
client = bigquery.Client(project='dataproject-blablacar')

# Detalles de la tabla en BigQuery. Sustitumos por el nombre del dataset y la tabla de GCP.
dataset_id = 'dataset_st'
table_id = 'vehiculos'
tabla_bigquery = f'{client.project}.{dataset_id}.{table_id}'

def generate_license_plate():
    num_random = ''.join(str(random.randint(0, 9)) for _ in range(4))
    let_random = ''.join(random.choice(string.ascii_uppercase) for _ in range(3))
    return f"{num_random}{let_random}"
       
num_rows = 5  # numero de filas que queremos generar
data = []
for _ in range(num_rows):
    row = [
        fake.random_int(1, 6),
        fake.name(),
        fake.vehicle_model(),
        fake.vehicle_make(),
        fake.random_int(1990, 2023),
        fake.color_name(),
        generate_license_plate(),
        fake.random_int(2, 7),
        fake.date_between(start_date='-30d', end_date='+30d').strftime('%Y-%m-%d'),  # insurance_expiry_date
        fake.pyfloat(positive=True),  # price_per_seat
        fake.random_element(['available', 'booked', 'completed']),
        fake.random_element(['5', '4', '3', '2', '1']),
        fake.boolean(),
        fake.boolean()
    ]
    data.append(row)

# Esquema tabla
schema = [
    bigquery.SchemaField("car_id", "INTEGER"),
    bigquery.SchemaField("owner_name", "STRING"),
    bigquery.SchemaField("car_model", "STRING"),
    bigquery.SchemaField("car_brand", "STRING"),
    bigquery.SchemaField("manufacturing_year", "INTEGER"),
    bigquery.SchemaField("car_color", "STRING"),
    bigquery.SchemaField("license_plate", "STRING"),
    bigquery.SchemaField("seating_capacity", "INTEGER"),
    bigquery.SchemaField("insurance_expiry_date", "DATE"),
    bigquery.SchemaField("price_per_seat", "FLOAT"),
    bigquery.SchemaField("trip_status", "STRING"),
    bigquery.SchemaField("ratings", "INTEGER"),
    bigquery.SchemaField("air_conditioning", "BOOLEAN"),
    bigquery.SchemaField("smoking", "BOOLEAN")
]

# Crea la tabla si no existe
table = bigquery.Table(tabla_bigquery, schema=schema)
table = client.create_table(table, exists_ok=True)

# Inserta los datos en la tabla
errors = client.insert_rows(table, data)

if errors:
    print(f'Error al insertar datos en BigQuery: {errors}')
else:
    print('Datos insertados correctamente en BigQuery.')
