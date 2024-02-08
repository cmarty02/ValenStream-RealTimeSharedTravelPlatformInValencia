### Consume datos del topic y los muestra por consola 



from confluent_kafka import Consumer, KafkaError
import json
import streamlit as st

###EJECUTAR DESDE CONSOLA CON 'streamlit run consumer_rutas.py'
# Configuración del consumidor
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'python-consumer-group',
    'auto.offset.reset': 'latest'
}

# Crear el consumidor
consumer = Consumer(config)
topic = 'rutas'
consumer.subscribe([topic])




###############################################################################
#######################Configurar la aplicación Streamlit######################
###############################################################################

st.title("Datos de Kafka en Streamlit")

# Loop para consumir y mostrar datos
while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

    # Obtener el valor del mensaje
    value = msg.value().decode('utf-8')
    
    # Convertir el valor JSON a un diccionario
    data = json.loads(value)

    # Mostrar datos en Streamlit
    st.write("Nuevo mensaje recibido:")
    st.write(data)

# Cerrar el consumidor al salir
consumer.close()
