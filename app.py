import json
from datetime import date

import boto3
import streamlit as st


# -----------------------------
# Configuración
# -----------------------------
st.set_page_config(
    page_title="Reporte de Taxis",
    page_icon="🚕",
    layout="centered"
)


# -----------------------------
# Estilos
# -----------------------------
st.markdown("""
<style>

/* Fondo principal */
.stApp {
    background: linear-gradient(
        135deg,
        #e2e8f0 0%,
        #f8fafc 50%,
        #dbeafe 100%
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
    color: #020617 !important;
    font-size: 42px;
    font-weight: 800;
    letter-spacing: -1px;
    text-shadow: 0px 2px 4px rgba(0,0,0,0.15);
}


/* Subtitulo */
.subtitle {
    text-align: center;
    color: #334155 !important;
    font-size: 17px;
    font-weight: 600;
    margin-bottom: 35px;
}


/* Tarjeta */
[data-testid="stVerticalBlockBorderWrapper"] {

    background: #ffffff;

    padding: 32px;

    border-radius: 20px;

    border: 1px solid #cbd5e1;

    box-shadow:
        0 10px 30px rgba(15,23,42,0.12);

}


/* Encabezados internos */
h2, h3 {

    color:#0f172a !important;

    font-weight:700 !important;

}


/* Texto */
p {

    color:#334155;

}


/* Labels */
label {

    color:#1e293b !important;

    font-weight:700 !important;

    font-size:15px !important;

}


/* Selectores */
div[data-baseweb="select"] > div {

    background:white;

    border:1px solid #94a3b8;

    border-radius:10px;

}


/* Texto de select */
div[data-baseweb="select"] span {

    color:#0f172a !important;

}


/* Botón */
.stButton button {

    width:100%;

    height:50px;

    border-radius:12px;

    background:#2563eb;

    color:white;

    font-size:17px;

    font-weight:700;

    border:none;

    transition:0.2s;

}


.stButton button:hover {

    background:#1d4ed8;

    transform:translateY(-1px);

}


/* Alertas */
.stAlert {

    border-radius:12px;

}


/* Separador */
hr {

    border-color:#cbd5e1;

}

</style>
""", unsafe_allow_html=True)



# -----------------------------
# Título
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


    meses = {
        "Enero": 1,
        "Febrero": 2,
        "Marzo": 3,
        "Abril": 4,
        "Mayo": 5,
        "Junio": 6,
        "Julio": 7,
        "Agosto": 8,
        "Septiembre": 9,
        "Octubre": 10,
        "Noviembre": 11,
        "Diciembre": 12
    }


    col1, col2 = st.columns(2)


    with col1:

        mes_nombre = st.selectbox(
            "Mes",
            list(meses.keys()),
            index=date.today().month - 1
        )


    with col2:

        anio = st.selectbox(
            "Año",
            list(range(2024, 2031)),
            index=list(range(2024, 2031)).index(date.today().year)
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


        st.info("Enviando información a AWS Lambda...")


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