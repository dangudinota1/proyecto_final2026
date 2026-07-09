# Imagen base
FROM python:3.11-slim
 
# Paquetes del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    awscli ca-certificates \
&& rm -rf /var/lib/apt/lists/*
 
# Dependencias Python (nota: usa python-dotenv, no "dotenv")
RUN pip install --no-cache-dir \
    streamlit \
    mysql-connector-python \
    pandas \
    matplotlib \
    seaborn \
    python-dotenv \
    boto3
 
# Directorio de trabajo
WORKDIR /app
 
# Copiar tu app
COPY app.py /app/
 
# Puerto
EXPOSE 8501
 
# Comando
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]