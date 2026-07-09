import json
from datetime import date

import boto3
import streamlit as st


# -----------------------------
# Configuración página
# -----------------------------
st.set_page_config(
    page_title="Generador de Reportes AWS",
    page_icon="🚀",
    layout="centered"
)


# -----------------------------
# CSS personalizado
# -----------------------------
st.markdown("""
<style>

/* Fondo general */
.stApp {
    background: linear-gradient(
        135deg,
        #0f172a,
        #1e3a8a,
        #2563eb
    );
}

/* Ocultar menú y footer */
#MainMenu {
    visibility: hidden;
}

footer {
    visibility: hidden;
}


/* Título */
.title {
    color: white;
    text-align: center;
    font-size: 45px;
    font-weight: 800;
    margin-bottom: 10px;
}


/* Subtitulo */
.subtitle {
    color: #dbeafe;
    text-align: center;
    font-size: 20px;
    margin-bottom: 35px;
}


/* Caja principal */
.block-container {
    padding-top: 3rem;
}


/* Card del formulario */
[data-testid="stVerticalBlockBorderWrapper"] {

    background: rgba(255,255,255,0.95);
    border-radius: 20px;
    padding: 30px;
    box-shadow:
        0px 10px 30px rgba(0,0,0,0.35);

}


/* Labels */
label {
    font-weight: 700 !important;
    color: #1e293b !important;
}


/* Botón */
.stButton button {

    width: 100%;
    height: 55px;

    background: linear-gradient(
        90deg,
        #22c55e,
        #16a34a
    );

    color:white;
    font-size:20px;
    font-weight:bold;

    border-radius:15px;
    border:none;

    transition:0.3s;

}


.stButton button:hover {

    background: linear-gradient(
        90deg,
        #16a34a,
        #15803d
    );

    transform: scale(1.02);

}


/* Mensajes */
.stAlert {

    border-radius:15px;

}

</style>
""", unsafe_allow_html=True)


# -----------------------------
# Encabezado
# -----------------------------
st.markdown(
    "<div class='title'>🚀 Generador de Reportes AWS</div>",
    unsafe_allow_html=True
)

st.markdown(
    """
    <div class='subtitle'>
    Selecciona los parámetros y envía la información a AWS Lambda
    </div>
    """,
    unsafe_allow_html=True
)



# -----------------------------
# Formulario
# -----------------------------
with st.container(border=True):

    col1, col2 = st.columns(2)


    with col1:

        mes = st.selectbox(
            "📅 Selecciona el mes",
            [
                "Enero",
                "Febrero",
                "Marzo",
                "Abril",
                "Mayo",
                "Junio",
                "Julio",
                "Agosto",
                "Septiembre",
                "Octubre",
                "Noviembre",
                "Diciembre"
            ],
            index=date.today().month-1
        )


    with col2:

        anio = st.selectbox(
            "🗓 Selecciona el año",
            list(range(2024,2031)),
            index=2
        )


    color = st.selectbox(
        "🎨 Selecciona color del reporte",
        [
            "🟡 Amarillo",
            "🟢 Verde"
        ]
    )


    st.write("")


    if st.button("🚀 Ejecutar Lambda"):


        payload = {

            "mes": mes,
            "anio": anio,
            "color": color.split(" ")[1].lower()

        }


        st.info("Procesando solicitud...")


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


            result = json.loads(
                response["Payload"].read()
            )


            st.success(
                "✅ Lambda ejecutada correctamente"
            )


            st.json(result)


        except Exception as e:

            st.error(
                f"❌ Error ejecutando Lambda: {e}"
            )