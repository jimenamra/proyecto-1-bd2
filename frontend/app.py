import streamlit as st
import requests

st.title("Gestor de archivos")

backend = "http://localhost:8000"

st.header("📥 Insertar valor")
val = st.number_input("Valor", step=1)
if st.button("Insertar"):
    r = requests.post(f"{backend}/insert", json={"value": val})
    st.success(r.json())

st.header("🔎 Búsqueda exacta")
key = st.number_input("Clave a buscar", step=1)
if st.button("Buscar clave"):
    r = requests.get(f"{backend}/search/{key}")
    st.json(r.json())

st.header("🔍 Búsqueda por rango")
a = st.number_input("Inicio", step=1)
b = st.number_input("Fin", step=1)
if st.button("Buscar por rango"):
    r = requests.get(f"{backend}/range/{a}/{b}")
    st.json(r.json())

st.header("❌ Eliminar")
delkey = st.number_input("Clave a eliminar", step=1)
if st.button("Eliminar"):
    r = requests.delete(f"{backend}/delete/{delkey}")
    st.success(r.json()['message'])

st.header("🧠 Ejecutar SQL")
sql = st.text_area("Comando SQL")
if st.button("Ejecutar SQL"):
    r = requests.post(f"{backend}/sql", params={"sql": sql})
    st.json(r.json())
