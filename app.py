import streamlit as st
import requests

st.title("Frontend de Base de Datos ISAM")
sql = st.text_area("Consulta SQL:")

if st.button("Ejecutar"):
    try:
        response = requests.post("http://localhost:8000/query/", json={"sql": sql})
        if response.status_code == 200:
            st.write(response.json())
        else:
            st.error(response.json()["detail"])
    except Exception as e:
        st.error(f"Error: {e}")
