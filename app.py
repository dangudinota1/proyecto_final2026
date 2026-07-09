import json
from datetime import date

import boto3
import streamlit as st


# ---------------------------------
# Configuración de la aplicación
# ---------------------------------
st.set_page_config(
    page_title="Reporte de Taxis",
    page_icon="🚕",
    layout="centered"
)


# ---------------------------------
# Bootstrap + estilos personalizados
# ---------------------------------
st.markdown(
    """
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
    rel="stylesheet">

    <style>

    /* Fondo */
    .stApp {
        background:
        linear-gradient(
            135deg,
            #e2e8f0,
            #f8fafc
        );
    }


    /* Contenedor */
    .block-container {

        max-width:720px;

        padding-top:3rem;

    }


    /* Título */
    .main-title {

        text-align:center;

        color:#020617;

        font-size:42px;

        font-weight:800;

        margin-bottom:10px;

    }


    /* Descripción */
    .subtitle {

        text-align:center;

        color:#334155;

        font-size:17px;

        margin-bottom:35px;

    }


    /* Card */
    .card-custom {

        background:white;

        border-radius:20px;

        padding:35px;

        box-shadow:
        0 10px 25px rgba(15,23,42,.12);

        border:
        1px solid #cbd5e1;

    }


    /* Labels */
    label {

        color:#0f172a !important;

        font-weight:700 !important;

    }


    /* Selectores */
    div[data-baseweb="select"] > div {

        background:white;

        border-radius:10px;

        border:1px solid #94a3b8;

    }


    /* Botón */
    .stButton button {

        width:100%;

        height:50px;

        border-radius:12px;

        background:#2563eb;

        color:white;

        font-weight:700;

        font-size:17px;

        border:none;

    }


    .stButton button:hover {

        background:#1d4ed8;

        color:white;

    }


    /* Alertas */

    .stAlert {

        border-radius:12px;

    }


    </style>
    """,
    unsafe_allow_html=True
)



# ---------------------------------
# Encabezado
# ---------------------------------

st.markdown(
    """
    <div class="main-title">
        🚕 Reporte de Taxis
    </div>

    <div class="subtitle">
        Selecciona el periodo y el color del taxi para ejecutar el proceso.
    </div>
    """,
    unsafe_allow_html=True
)



# ---------------------------------
# Formulario
# ---------------------------------

st.markdown(
    """
    <div class="card-custom">
    """,
    unsafe_allow_html=True
)


st.markdown(
    """
    <h4 style="color:#020617;">
        📅 Periodo
    </h4>
    """,
    unsafe_allow_html=True
)



meses = {
    "Enero":1,
    "Febrero":2,
    "Marzo":3,
    "Abril":4,
    "Mayo":5,
    "Junio":6,
    "Julio":7,
    "Agosto":8,
    "Septiembre":9,
    "Octubre":10,
    "Noviembre":11,
    "Diciembre":12
}



col1, col2 = st.columns(2)


with col1:

    mes_nombre = st.selectbox(
        "Mes",
        list(meses.keys()),
        index=date.today().month-1
    )


with col2:

    anio = st.selectbox(
        "Año",
        list(range(2024,2031)),
        index=list(range(2024,2031)).index(date.today().year)
    )



st.divider()



color = st.selectbox(
    "🎨 Color de taxi",
    [
        "Amarillo",
        "Verde"
    ]
)



st.write("")



# ---------------------------------
# Lambda
# ---------------------------------

if st.button("🚀 Ejecutar Lambda"):


    payload = {

        "mes": meses[mes_nombre],

        "anio": anio,

        "color_taxi": color.lower()

    }


    st.info(
        "Enviando parámetros a AWS Lambda..."
    )


    try:


        lambda_client = boto3.client(

            "lambda",

            region_name="us-east-1"

        )


        response = lambda_client.invoke(

            FunctionName="NOMBRE_DE_TU_LAMBDA",

            InvocationType="RequestResponse",

            Payload=json.dumps(payload)

        )


        resultado = json.loads(

            response["Payload"].read()

        )


        st.success(
            "Proceso ejecutado correctamente"
        )


        st.json(resultado)



    except Exception as e:


        st.error(
            f"Error ejecutando Lambda: {e}"
        )



st.markdown(
    """
    </div>
    """,
    unsafe_allow_html=True
)