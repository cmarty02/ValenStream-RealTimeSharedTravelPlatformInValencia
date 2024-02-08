# create_table.py
import psycopg2
import time



# Retrasa la ejecucion del script 20 segundos
time.sleep(15)



dbname = "DBlablaCar"
user = "postgres"
password = "postgres"
host = "postgres" #localhost si lo ejecutamos manualmente 
port = "5432"


# Crear una conexión
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)


# Cursor para ejecutar consultas SQL
cursor = conn.cursor()

# Crear la tabla (ajusta esto según tus necesidades)
cursor.execute('''
    CREATE TABLE rutas (
        index INT,
        latitud VARCHAR(255),
        longitud VARCHAR(255),
        tipo_ruta VARCHAR(255),
        Origen_destino VARCHAR(255) 
    );
''')

# Confirmar la creación de la tabla
conn.commit()

# Cerrar la conexión y el cursor
cursor.close()
conn.close()