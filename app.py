import json
from datetime import date

import boto3
import streamlit as st

# Configuración de la página
st.set_page_config(page_title="Invocar Lambda", page_icon="☁️")

st.title("Invocar AWS Lambda")
st.write("Selecciona un mes, un año y un color.")

# Selector de fecha (se utiliza únicamente el mes y el año)
fecha = st.date_input(
    "Selecciona el mes y año",
    value=date.today()
)

# Extraer mes y año
mes = fecha.month
anio = fecha.year

# Selector de color
color = st.selectbox(
    "Selecciona un color",
    ["Amarillo", "Verde"]
)

if st.button("Enviar a Lambda"):

    # Crear cliente de Lambda
    lambda_client = boto3.client(
        "lambda",
        region_name="us-east-1"  # Cambia por tu región
    )

    payload = {
        "mes": mes,
        "anio": anio,
        "color": color.lower()
    }

    try:
        response = lambda_client.invoke(
            FunctionName="NOMBRE_DE_TU_LAMBDA",  # O el ARN
            InvocationType="RequestResponse",
            Payload=json.dumps(payload)
        )

        resultado = json.loads(response["Payload"].read())

        st.success("Lambda ejecutada correctamente")
        st.write(resultado)

    except Exception as e:
        st.error(f"Error al invocar la Lambda: {e}")