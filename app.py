import json
from datetime import date

import boto3
import streamlit as st

# -----------------------------
# Configuración de la página
# -----------------------------
st.set_page_config(
    page_title="Generador de Reportes",
    page_icon="📊",
    layout="centered"
)

# -----------------------------
# Estilos CSS
# -----------------------------
st.markdown("""
<style>
.main-title{
    font-size:40px;
    font-weight:bold;
    color:#0E6EFD;
    text-align:center;
}
.subtitle{
    font-size:18px;
    color:#6c757d;
    text-align:center;
    margin-bottom:30px;
}
.stButton>button{
    width:100%;
    background-color:#0E6EFD;
    color:white;
    border-radius:10px;
    height:50px;
    font-size:18px;
}
.stButton>button:hover{
    background-color:#0b5ed7;
}
</style>
""", unsafe_allow_html=True)

st.markdown("<p class='main-title'>📊 Generador de Reportes</p>", unsafe_allow_html=True)
st.markdown(
    "<p class='subtitle'>Selecciona el período y el color del reporte para ejecutar la Lambda.</p>",
    unsafe_allow_html=True
)

# -----------------------------
# Formulario
# -----------------------------
with st.container(border=True):

    col1, col2 = st.columns(2)

    with col1:
        mes = st.selectbox(
            "📅 Mes",
            [
                "Enero", "Febrero", "Marzo", "Abril",
                "Mayo", "Junio", "Julio", "Agosto",
                "Septiembre", "Octubre", "Noviembre", "Diciembre"
            ],
            index=date.today().month - 1
        )

    with col2:
        anio = st.selectbox(
            "🗓️ Año",
            list(range(2023, 2031)),
            index=date.today().year - 2023
        )

    color = st.selectbox(
        "🎨 Color",
        ["🟡 Amarillo", "🟢 Verde"]
    )

    st.divider()

    if st.button("🚀 Ejecutar proceso"):

        payload = {
            "mes": mes,
            "anio": anio,
            "color": color.split(" ")[1].lower()
        }

        st.info("Enviando solicitud a AWS Lambda...")

        try:
            client = boto3.client(
                "lambda",
                region_name="us-east-1"
            )

            response = client.invoke(
                FunctionName="NOMBRE_DE_TU_LAMBDA",
                InvocationType="RequestResponse",
                Payload=json.dumps(payload)
            )

            resultado = json.loads(response["Payload"].read())

            st.success("✅ Proceso ejecutado correctamente")

            st.subheader("Respuesta de la Lambda")
            st.json(resultado)

        except Exception as e:
            st.error(f"❌ Error: {e}")