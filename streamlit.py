import streamlit as st

# Título de la aplicación
st.title("Aplicación de Saludo Personalizado")

# Entrada del usuario
nombre = st.text_input("Ingresa tu nombre:")

# Botón para mostrar el saludo
if st.button("Saludar"):
    if nombre:
        st.write(f"¡Hola, {nombre}! Bienvenido a la aplicación.")
    else:
        st.write("Por favor, ingresa tu nombre.")