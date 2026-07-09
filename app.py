import streamlit as st
import pandas as pd
st.title("Ejemplo de titanic")
st.write("Este es un streamlite que habla sobre titanic")
dataset = st.text_input("Data de titanic", value="https://...")
if st.button("Cargar data"):
    df = pd.read_csv(dataset)
    st.write("Filas:", df.shape[0])
    st.write("Columnas:", df.shape[1])