### Consume datos del topic y los envia a una base de datos llamada DBlablaCar


# consumer.py
from confluent_kafka import Consumer, KafkaError
import psycopg2
import json

# Configuración del consumidor de Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'kafka-consumer-group',
    'auto.offset.reset': 'earliest'  # Puedes ajustar esto según tus necesidades
}

# Crear el consumidor de Kafka
consumer = Consumer(conf)

kafka_topic= 'rutas'
consumer.subscribe([kafka_topic])


dbname = "DBlablaCar"
user = "postgres"
password = "postgres"
host = "localhost"
port = "5432"

# Crear una conexión
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)



###############################################################################
####################### INSERT index, latitud,longitud IN BBDD ################
###############################################################################

# Cursor para ejecutar consultas SQL
cursor = conn.cursor()

try:
    while True:
        msg = consumer.poll(1.0)  # Esperar durante 1 segundo por nuevos mensajes
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaError(msg.error())

        # Procesar el mensaje JSON y extraer los campos
        try:
            data = json.loads(msg.value().decode('utf-8'))
            index = data.get('index')  
            latitud = data.get('latitud') 
            longitud = data.get('longitud')
            tipo_ruta = data.get('tipo_ruta') 
            Origen_destino = data.get('nombre_ruta')

            # Insertar en la tabla con dos columnas
            cursor.execute("INSERT INTO rutas (index,latitud, longitud,tipo_ruta,Origen_destino) VALUES (%s, %s, %s, %s, %s);", (index, latitud,longitud,tipo_ruta,Origen_destino))
            conn.commit()
            print(f"Mensaje insertado en la base de datos: {data}")

        except json.JSONDecodeError as e:
            print(f"Error al decodificar el mensaje JSON: {e}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    cursor.close()
    conn.close()