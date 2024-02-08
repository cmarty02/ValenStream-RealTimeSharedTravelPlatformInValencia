# Utiliza una imagen base de Python 3.11
FROM python:3.11.4

# Configura el directorio de trabajo
WORKDIR /app

# Copia el archivo requirements.txt a la imagen
COPY requirements.txt .

# Instalar las dependencias y limpiar
RUN pip install --no-cache-dir -r requirements.txt \
    pip install confluent_kafka\
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copiar los scripts
COPY bbdd/create_table.py .
#COPY producer_ruta.py .
#COPY consumer_bbdd.py .

# Establecer el comando predeterminado
CMD ["bash", "-c", "python create_table.py && python producer_ruta.py && python consumer_bbdd.py"]

