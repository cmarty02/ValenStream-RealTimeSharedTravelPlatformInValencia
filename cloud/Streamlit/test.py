import streamlit as st

# Solicitar al usuario que ingrese un número
numero_ingresado = st.number_input("Por favor, ingresa un número para acceder a las pestañas:")

# Verificar si el número ingresado es correcto
if numero_ingresado == 123:
    # Creamos las pestañas con los nombres respectivos
    tab1, tab2, tab3 = st.columns([1, 1, 1])

    # Añadimos contenido a cada pestaña
    with tab1:
        st.title("Pestaña 1")
        st.write("Contenido de la pestaña 1")

    with tab2:
        st.title("Pestaña 2")
        st.write("Contenido de la pestaña 2")

    with tab3:
        st.title("Pestaña 3")
        st.write("Contenido de la pestaña 3")
else:
    st.write("Número incorrecto. Por favor, ingresa el número correcto para acceder a las pestañas.")
