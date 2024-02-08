
#Copiar app al contenedor de docker. Reemplazar con ID del contenedor. 
#docker cp -L app.py 338ac7177fa7:/opt/bitnami/spark/app.py


#Ejecutar app dentro contenedor de docker(nodo Spark). Reemplazar con ID del contenedor. 
#docker exec 338ac7177fa7 spark-submit --master spark://spark-master:7077 app.py

#Se deberia ver un dataframe en la consola

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

# Create a SparkSession
spark = SparkSession.builder \
    .appName("My App") \
    .getOrCreate()

# Crear un DataFrame con 100 filas y una columna llamada 'random_data'
df = spark.range(1, 101).select(rand().alias('random_data'))

# Mostrar el DataFrame
df.show()

# Stop the SparkSession
spark.stop()


"""""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Crea una SparkSession
spark = SparkSession.builder \
    .appName("KafkaStreamingApp") \
    .getOrCreate()

# Configura la fuente de streaming desde Kafka
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'rutas'

# Define el esquema para los datos JSON
json_schema = StructType([
    StructField('index', StringType(), True),
    StructField('latitud', DoubleType(), True),
    StructField('longitud', DoubleType(), True)
])

# Configura las opciones de Kafka
kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap_servers,
    "subscribe": kafka_topic,
    "startingOffsets": "earliest"
}

# Lee datos desde Kafka utilizando spark-sql-kafka-0-10
streaming_df = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()

# Convierte los datos JSON en un DataFrame estructurado
streaming_df = streaming_df.selectExpr("CAST(value AS STRING)")

parsed_df = streaming_df.select(from_json("value", json_schema).alias("data"))

# Explode para separar la estructura anidada
streaming_df = parsed_df.select("data.*")

# Muestra el resultado por consola en modo de streaming
query = streaming_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Espera a que el streaming termine
query.awaitTermination()
"""