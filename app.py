import json
from datetime import date

import boto3
import streamlit as st


# =====================================
# CONFIGURACIÓN
# =====================================

st.set_page_config(
    page_title="Taxi Analytics",
    page_icon="🚕",
    layout="centered"
)


# =====================================
# BOOTSTRAP CSS
# =====================================

st.markdown(
"""
<link
href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
rel="stylesheet">

<style>

/* Fondo */
.stApp {

    background:
    linear-gradient(
        120deg,
        #f1f5f9,
        #dbeafe
    );

}


/* Contenedor */
.block-container {

    max-width:850px;

    padding-top:2rem;

}


/* Navbar */

.navbar-custom {

    background:#0f172a;

    padding:20px;

    border-radius:18px;

    margin-bottom:30px;

    box-shadow:
    0 8px 20px rgba(0,0,0,.15);

}


.navbar-title {

    color:white;

    font-size:28px;

    font-weight:800;

}


.navbar-text {

    color:#cbd5e1;

    font-size:14px;

}


/* Card */

.card-custom {

    background:white;

    border-radius:20px;

    padding:35px;

    box-shadow:
    0 10px 30px rgba(15,23,42,.12);

}


/* Títulos */

.section-title {

    color:#0f172a;

    font-size:22px;

    font-weight:700;

}


/* Labels */

label {

    color:#1e293b !important;

    font-weight:700 !important;

}


/* Select */

div[data-baseweb="select"] > div {

    border-radius:12px !important;

    border:1px solid #94a3b8;

}


/* Botón */

.stButton button {

    width:100%;

    height:52px;

    background:#2563eb;

    color:white;

    border-radius:12px;

    border:none;

    font-weight:700;

    font-size:17px;

}


.stButton button:hover {

    background:#1d4ed8;

}


/* Alertas */

.stAlert {

    border-radius:12px;

}

</style>
""",
unsafe_allow_html=True
)


# =====================================
# HEADER
# =====================================

st.markdown(
    """
    <div style="
        background:#0f172a;
        padding:20px;
        border-radius:18px;
        margin-bottom:30px;
        box-shadow:0 8px 20px rgba(0,0,0,.15);
    ">
        <h2 style="color:white;margin:0;">
            🚕 Taxi Analytics Platform
        </h2>

        <span style="color:#cbd5e1;">
            AWS Lambda Report Generator
        </span>
    </div>
    """,
    unsafe_allow_html=True
)


# =====================================
# CARD PRINCIPAL
# =====================================

st.markdown(
"""
<div class="card-custom">

<div class="section-title">
📅 Parámetros del reporte
</div>

<br>

""",
unsafe_allow_html=True
)


# =====================================
# PERIODO
# =====================================

meses = {
    "Enero": "01",
    "Febrero": "02",
    "Marzo": "03",
    "Abril": "04",
    "Mayo": "05",
    "Junio": "06",
    "Julio": "07",
    "Agosto": "08",
    "Septiembre": "09",
    "Octubre": "10",
    "Noviembre": "11",
    "Diciembre": "12"
}

col1, col2 = st.columns(2)

with col1:

    mes_nombre = st.selectbox(
        "Periodo - Mes",
        list(meses.keys()),
        index=date.today().month - 1
    )

with col2:

    anio = st.selectbox(
        "Periodo - Año",
        range(2024, 2031),
        index=list(range(2024, 2031)).index(date.today().year)
    )

st.write("")


# =====================================
# COLOR TAXI
# =====================================

colores = {
    "Amarillo": "yellow",
    "Verde": "green"
}

color_seleccionado = st.selectbox(
    "🚕 Color de taxi",
    list(colores.keys())
)

# Valor que se enviará a la Lambda
color_taxi = colores[color_seleccionado]

st.write("")


# =====================================
# EJECUTAR LAMBDA
# =====================================

if st.button("🚀 Ejecutar procesamiento"):

    payload = {
        "mes": meses[mes_nombre],
        "anio": anio,
        "color_taxi": color_taxi
    }

    st.info("Ejecutando Lambda...")

    try:

        lambda_client = boto3.client(
            "lambda",
            region_name="us-west-1"
        )

        response = lambda_client.invoke(
            FunctionName="arn:aws:lambda:us-west-1:020635523025:function:xideral_daniel_lambda_proyectofinal2026",
            InvocationType="RequestResponse",
            Payload=json.dumps(payload)
        )

        result = json.loads(
            response["Payload"].read()
        )

        st.success("✅ Proceso completado correctamente")

        st.subheader("Respuesta de la Lambda")

        st.json(result)
        

    except Exception as error:

        st.error(f"❌ Error: {error}")


st.markdown(
"""
</div>
""",
unsafe_allow_html=True
)