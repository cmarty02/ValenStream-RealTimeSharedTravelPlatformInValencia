# Ejemplo de gráfico de barras
import pandas as pd
import matplotlib.pyplot as plt

#aqui pondriamos los datos que queremos leer.

df= pd.read_json('datos.json')  # Suponiendo que tienes un archivo CSV con tus datos

st.subheader('Visualización de datos')
st.write("Aquí está un gráfico de barras simple para mostrar la distribución de precios de los viajes:")

fig, ax = plt.subplots()
ax.bar(df['Ruta'], df['Precio'])
plt.xticks(rotation=45)
st.pyplot(fig)
