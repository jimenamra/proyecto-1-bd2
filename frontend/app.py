import streamlit as st
import requests
import pandas as pd

st.set_page_config(page_title="MultiDB Manager", page_icon="💾")
st.title("💾 MultiDB Manager")
backend = "http://localhost:8000"

tabs = st.tabs(["📥 Agregar","🔎 Buscar", "❌ Eliminar", "🧠 SQL Parser"])

# ========== 📥 TAB: AGREGAR ==========
with tabs[0]:
    st.subheader("➕ Registrar un nuevo desastre")
    id = st.number_input("ID del desastre (entero)", step=1, format="%d")
    fecha = st.text_input("Fecha (YYYY-MM-DD)")
    tipo = st.selectbox("Tipo", ["Earthquake", "Flood", "Tsunami", "Landslide", "Other"])
    lat = st.number_input("Latitud", format="%.6f")
    lon = st.number_input("Longitud", format="%.6f")
    mag = st.number_input("Magnitud", format="%.1f")
    prof = st.number_input("Profundidad", format="%.1f")

    if st.button("Registrar desastre", type="primary"):
        payload = {
            "id": id,
            "fecha": fecha,
            "tipo": tipo,
            "lat": lat,
            "lon": lon,
            "mag": mag,
            "prof": prof
        }
        r = requests.post(f"{backend}/insert", json=payload)
        if r.status_code == 200:
            st.json(r.json())
        else:
            st.error(f"Error: {r.text}")

# ========== 🔎 TAB: BUSCAR ==========
with tabs[1]:
    st.subheader("🔑 Búsqueda Exacta")
    key = st.number_input("ID del desastre a buscar", step=1, format="%d")
    if st.button("Buscar ID", type="primary"):
        r = requests.get(f"{backend}/search/{key}")
        st.json(r.json())

    st.subheader("📊 Búsqueda por Rango")
    col1, col2 = st.columns(2)
    with col1:
        a = st.number_input("Inicio del rango", step=1, format="%d")
    with col2:
        b = st.number_input("Fin del rango", step=1, format="%d")
    if st.button("Buscar por rango", type="primary"):
        r = requests.get(f"{backend}/range/{a}/{b}")
        st.json(r.json())

    st.subheader(" Búsqueda por rango de área")
    col1, col2 = st.columns(2)
    with col1:
        a1 = st.number_input("Ingresar Latitud", step=0.0001, format="%.6f")
    with col2:
        b1 = st.number_input("Ingresar Longitud", step=0.0001, format="%.6f")
    if st.button("Buscar por rango de area", type="primary", key="buscar_rango_btn"):
        r = requests.get(f"{backend}/range_area/{a1}/{b1}")
        st.json(r.json())


# ========== ❌ TAB: ELIMINAR ==========
with tabs[2]:
    st.subheader("Eliminar registro")
    delkey = st.number_input("Clave a eliminar", step=1, key="delete_key")
    if st.button("Eliminar", type="primary"):
        r = requests.delete(f"{backend}/delete/{delkey}")
        st.json(r.json())

# ========== 🧠 TAB: SQL PARSER ==========
with tabs[3]:
    st.subheader("📁 Cargar archivo CSV")
    uploaded_file = st.file_uploader("Selecciona un archivo CSV", type=["csv"])
    if uploaded_file:
        df = pd.read_csv(uploaded_file)
        st.write("Vista previa del archivo:")
        st.dataframe(df.head())

        if st.button("Cargar y guardar CSV", type="primary"):
            files = {"file": (uploaded_file.name, uploaded_file.getvalue(), "text/csv")}
            response = requests.post(f"{backend}/upload_csv", files=files)
            if response.status_code == 200:
                st.success("Archivo guardado exitosamente en el backend.")
            else:
                st.error("Error al guardar el archivo en el backend.")

    st.subheader("Ejecutar comando SQL")
    sql = st.text_area("Escribe una sentencia SQL", height=200)
    if st.button("Ejecutar SQL", type="primary"):
        r = requests.post(f"{backend}/sql", params={"sql": sql})
        try:
            json_data = r.json()
            st.json(json_data)
        except Exception as e:
            st.error(f"Error al procesar la respuesta del backend.")
            st.code(r.text)
