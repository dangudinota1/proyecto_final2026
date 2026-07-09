import json
from datetime import date

import boto3
import streamlit as st


# -----------------------------
# Configuración
# -----------------------------
st.set_page_config(
    page_title="AWS Lambda Trigger",
    page_icon="☁️",
    layout="centered"
)


# -----------------------------
# Estilos minimalistas
# -----------------------------
st.markdown("""
<style>

/* Fondo */
.stApp {
    background-color: #f8fafc;
}


/* Contenedor principal */
.block-container {
    max-width: 700px;
    padding-top: 3rem;
}


/* Título */
h1 {
    color: #111827;
    text-align: center;
    font-size: 36px;
    font-weight: 700;
}


/* Texto */
.description {
    text-align: center;
    color: #6b7280;
    font-size: 16px;
    margin-bottom: 30px;
}


/* Caja del formulario */
[data-testid="stVerticalBlockBorderWrapper"] {

    background-color: white;

    border-radius: 16px;

    padding: 25px;

    border: 1px solid #e5e7eb;

    box-shadow: 
        0 4px 12px rgba(0,0,0,0.05);

}


/* Inputs */
label {

    color:#374151 !important;

    font-weight:600 !important;

}


/* Botón */
.stButton button {

    width:100%;

    background-color:#2563eb;

    color:white;

    border-radius:10px;

    height:45px;

    font-weight:600;

    border:none;

}


.stButton button:hover {

    background-color:#1d4ed8;

}


/* Alertas */
.stAlert {

    border-radius:10px;

}

</style>
""", unsafe_allow_html=True)



# -----------------------------
# Encabezado
# -----------------------------
st.title("☁️ Ejecución AWS Lambda")

st.markdown(
    """
    <div class="description">
    Selecciona la fecha y el color para enviar los parámetros al servicio.
    </div>
    """,
    unsafe_allow_html=True
)



# -----------------------------
# Formulario
# -----------------------------
with st.container(border=True):

    # Calendario
    fecha = st.date_input(
        "📅 Selecciona fecha",
        value=date.today()
    )


    # Extraer solamente mes y año
    mes = fecha.month
    anio = fecha.year


    # Color
    color = st.selectbox(
        "🎨 Selecciona color",
        [
            "Amarillo",
            "Verde"
        ]
    )


    st.write("")


    if st.button("Enviar parámetros"):


        payload = {

            "mes": mes,
            "anio": anio,
            "color": color.lower()

        }


        st.info(
            "Enviando información a Lambda..."
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
                "Solicitud procesada correctamente"
            )


            st.json(resultado)


        except Exception as e:

            st.error(
                f"Error: {e}"
            )