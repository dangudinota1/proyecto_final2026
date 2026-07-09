import json
import boto3
import streamlit as st
from datetime import date


# -----------------------------
# Configuración
# -----------------------------
st.set_page_config(
    page_title="Generador de Reportes",
    page_icon="🚕",
    layout="centered"
)


# -----------------------------
# Estilos
# -----------------------------
st.markdown("""
<style>

/* Fondo general */
.stApp {
    background: linear-gradient(
        135deg,
        #eef2ff,
        #f8fafc
    );
}


/* Contenedor */
.block-container {
    max-width: 700px;
    padding-top: 3rem;
}


/* Título */
h1 {
    text-align: center;
    color: #1e293b;
    font-size: 38px;
    font-weight: 700;
}


/* Subtitulo */
.subtitle {

    text-align:center;
    color:#64748b;
    font-size:16px;
    margin-bottom:30px;

}


/* Tarjeta */
[data-testid="stVerticalBlockBorderWrapper"] {

    background:white;

    padding:30px;

    border-radius:18px;

    border:1px solid #e2e8f0;

    box-shadow:
    0px 8px 25px rgba(15,23,42,0.08);

}


/* Labels */
label {

    color:#334155 !important;

    font-weight:600 !important;

}


/* Botón */
.stButton button {

    width:100%;

    height:48px;

    border-radius:12px;

    background:#2563eb;

    color:white;

    font-size:16px;

    font-weight:600;

    border:none;

}


.stButton button:hover {

    background:#1d4ed8;

}


/* Separador */
hr {

    border-color:#e2e8f0;

}


</style>
""", unsafe_allow_html=True)



# -----------------------------
# Encabezado
# -----------------------------
st.title("🚕 Reporte de Taxis")

st.markdown(
    """
    <div class="subtitle">
    Selecciona el periodo y el color del taxi para ejecutar el proceso.
    </div>
    """,
    unsafe_allow_html=True
)



# -----------------------------
# Formulario
# -----------------------------
with st.container(border=True):


    st.subheader("📅 Periodo")


    col1, col2 = st.columns(2)


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


    with col1:

        mes_nombre = st.selectbox(
            "Mes",
            list(meses.keys()),
            index=date.today().month-1
        )


    with col2:

        anio = st.selectbox(
            "Año",
            list(range(2020,2031)),
            index=list(range(2020,2031)).index(date.today().year)
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


    if st.button("Enviar parámetros"):


        payload = {

            "mes": meses[mes_nombre],
            "anio": anio,
            "color_taxi": color.lower()

        }


        st.info("Enviando datos a AWS Lambda...")


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