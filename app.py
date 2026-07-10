import json
from datetime import date, datetime, timezone
from pathlib import Path
import os
import urllib.request

import boto3
import streamlit as st
import pandas as pd

# =====================================
# CONFIGURACIÓN DE S3
# =====================================
S3_BUCKET = "xideralaws-curso-danielg2026"
S3_PARQUET_KEY = "raw/taxis/green/2025/01/green_tripdata_2025-01.parquet"
S3_KPIS_KEY = "nyc_taxi/kpis/green_tripdata_2025_01_kpis.json"

# Directorios locales
LOCAL_DIR = Path.home() / "data" / "nyc_taxi"
LOCAL_PARQUET = LOCAL_DIR / "raw" / "green_tripdata_2025-01.parquet"
LOCAL_KPIS_PATH = LOCAL_DIR / "kpis" / "green_tripdata_2025_01_kpis.json"
LOCAL_DIR.mkdir(parents=True, exist_ok=True)
(LOCAL_DIR / "raw").mkdir(parents=True, exist_ok=True)
(LOCAL_DIR / "kpis").mkdir(parents=True, exist_ok=True)

# URL HTTPS de fallback para el Parquet
HTTPS_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-01.parquet"

# =====================================
# FUNCIONES DE DESCARGA
# =====================================
def download_from_s3(bucket: str, key: str, destination: Path) -> bool:
    """Descarga un archivo desde S3 usando boto3."""
    try:
        if destination.exists() and destination.stat().st_size > 0:
            st.info(f"✅ Archivo ya existe localmente: {destination.name}")
            return True
        
        with st.spinner(f"Descargando archivo desde S3: {key}..."):
            s3 = boto3.client("s3")
            s3.download_file(bucket, key, str(destination))
            st.success(f"✅ Archivo descargado exitosamente: {destination.name}")
            return True
    except Exception as e:
        st.warning(f"⚠️ Error al descargar desde S3: {e}")
        return False

def download_from_https(url: str, destination: Path) -> bool:
    """Descarga desde HTTPS como fallback."""
    try:
        if destination.exists() and destination.stat().st_size > 0:
            st.info(f"✅ Archivo ya existe localmente: {destination.name}")
            return True
        
        with st.spinner("Descargando desde HTTPS..."):
            urllib.request.urlretrieve(url, destination)
            st.success("✅ Archivo descargado desde HTTPS")
            return True
    except Exception as e:
        st.error(f"❌ Error al descargar desde HTTPS: {e}")
        return False

def download_parquet() -> bool:
    """Descarga el archivo Parquet desde S3 o HTTPS."""
    # Intentar primero desde S3
    if download_from_s3(S3_BUCKET, S3_PARQUET_KEY, LOCAL_PARQUET):
        return True
    
    # Fallback a HTTPS
    st.warning("⚠️ Usando descarga HTTPS como alternativa...")
    return download_from_https(HTTPS_URL, LOCAL_PARQUET)

def download_kpis_from_s3() -> bool:
    """Descarga el JSON de KPIs desde S3."""
    try:
        if LOCAL_KPIS_PATH.exists() and LOCAL_KPIS_PATH.stat().st_size > 0:
            st.info(f"✅ KPIs ya existen localmente")
            return True
        
        with st.spinner("Descargando KPIs desde S3..."):
            s3 = boto3.client("s3")
            s3.download_file(S3_BUCKET, S3_KPIS_KEY, str(LOCAL_KPIS_PATH))
            st.success("✅ KPIs descargados desde S3")
            return True
    except Exception as e:
        st.warning(f"⚠️ No se pudieron descargar KPIs desde S3: {e}")
        return False

# =====================================
# FUNCIONES DE PROCESAMIENTO CON SPARK
# =====================================
def process_with_spark():
    """Procesa el archivo Parquet con Spark y genera KPIs."""
    try:
        from pyspark.sql import SparkSession, functions as F
        from pyspark.sql.types import NumericType
        
        # Crear SparkSession
        HOME = Path.home()
        SPARK_TMP = HOME / "spark-tmp"
        JAVA_TMP = HOME / "spark-tmp" / "java"
        
        for path in [SPARK_TMP, JAVA_TMP]:
            path.mkdir(parents=True, exist_ok=True)
        
        os.environ["TMPDIR"] = str(SPARK_TMP)
        os.environ["TEMP"] = str(SPARK_TMP)
        os.environ["TMP"] = str(SPARK_TMP)
        os.environ["SPARK_LOCAL_DIRS"] = str(SPARK_TMP)
        os.environ["_JAVA_OPTIONS"] = f"-Djava.io.tmpdir={JAVA_TMP}"
        
        try:
            spark.stop()
        except Exception:
            pass
        
        spark = (
            SparkSession.builder
            .appName("nyc-taxi-kpis")
            .master("local[*]")
            .config("spark.hadoop.fs.defaultFS", "file:///")
            .config("spark.sql.warehouse.dir", f"file://{HOME / 'spark-warehouse'}")
            .config("spark.local.dir", str(SPARK_TMP))
            .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={JAVA_TMP}")
            .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={JAVA_TMP}")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )
        
        spark.sparkContext.setLogLevel("WARN")
        
        # Leer el Parquet
        with st.spinner("Leyendo archivo Parquet con Spark..."):
            local_uri = f"file://{LOCAL_PARQUET}"
            df_raw = spark.read.parquet(local_uri)
        
        total_rows = df_raw.count()
        st.info(f"📊 Registros encontrados: {total_rows:,}")
        
        # Usar muestra para procesamiento más rápido
        USE_SAMPLE = True
        SAMPLE_FRACTION = 0.10
        
        if USE_SAMPLE:
            df = df_raw.sample(withReplacement=False, fraction=SAMPLE_FRACTION, seed=42)
            st.info(f"📊 Usando muestra del {SAMPLE_FRACTION*100:.0f}%")
        else:
            df = df_raw
        
        # Crear columnas auxiliares
        with st.spinner("Procesando datos..."):
            df_audit = (
                df
                .withColumn("pickup_ts", F.col("lpep_pickup_datetime").cast("timestamp"))
                .withColumn("dropoff_ts", F.col("lpep_dropoff_datetime").cast("timestamp"))
                .withColumn(
                    "duration_min",
                    (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60
                )
                .withColumn("pickup_date", F.to_date("pickup_ts"))
                .withColumn("pickup_hour", F.hour("pickup_ts"))
            )
            
            # Limpiar datos
            df_clean = (
                df_audit
                .filter(F.col("pickup_ts").isNotNull())
                .filter(F.col("dropoff_ts").isNotNull())
                .filter(F.col("dropoff_ts") > F.col("pickup_ts"))
                .filter((F.col("pickup_ts") >= F.lit("2025-01-01")) & (F.col("pickup_ts") < F.lit("2025-02-01")))
                .filter((F.col("duration_min") > 0) & (F.col("duration_min") <= 24 * 60))
                .filter((F.col("trip_distance") > 0) & (F.col("trip_distance") <= 100))
                .filter(F.col("passenger_count").between(1, 6))
                .filter(F.col("fare_amount") >= 0)
                .filter(F.col("total_amount") >= 0)
                .dropDuplicates()
                .cache()
            )
            
            clean_rows = df_clean.count()
            removed_rows = total_rows - clean_rows
            removed_pct = removed_rows / total_rows if total_rows else 0
            
            st.success(f"✅ Datos limpios: {clean_rows:,} registros ({removed_pct:.2%} removidos)")
            
            # Calcular KPIs
            st.info("📊 Calculando KPIs...")
            
            # KPI Resumen
            kpi_resumen = df_clean.agg(
                F.count("*").alias("total_trips"),
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                F.round(F.avg("total_amount"), 2).alias("avg_ticket"),
                F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
                F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
                F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
                F.round(
                    F.when(
                        F.sum("fare_amount") > 0,
                        100 * F.sum("tip_amount") / F.sum("fare_amount")
                    ).otherwise(0),
                    2
                ).alias("avg_tip_pct"),
                F.round(F.avg("passenger_count"), 2).alias("avg_passenger_count"),
            )
            
            # KPI por día
            kpi_por_dia = (
                df_clean
                .groupBy("pickup_date")
                .agg(
                    F.count("*").alias("trips"),
                    F.round(F.sum("total_amount"), 2).alias("revenue"),
                    F.round(F.avg("total_amount"), 2).alias("avg_ticket"),
                    F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
                    F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
                    F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
                )
                .orderBy("pickup_date")
            )
            
            # KPI por hora
            kpi_por_hora = (
                df_clean
                .groupBy("pickup_hour")
                .agg(
                    F.count("*").alias("trips"),
                    F.round(F.sum("total_amount"), 2).alias("revenue"),
                    F.round(F.avg("total_amount"), 2).alias("avg_ticket"),
                    F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
                    F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
                    F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
                )
                .orderBy("pickup_hour")
            )
            
            # KPI por tipo de pago
            kpi_tipo_pago = (
                df_clean
                .withColumn(
                    "payment_type_label",
                    F.when(F.col("payment_type") == 1, "Credit card")
                     .when(F.col("payment_type") == 2, "Cash")
                     .when(F.col("payment_type") == 3, "No charge")
                     .when(F.col("payment_type") == 4, "Dispute")
                     .when(F.col("payment_type") == 5, "Unknown")
                     .otherwise("Other / null")
                )
                .groupBy("payment_type", "payment_type_label")
                .agg(
                    F.count("*").alias("trips"),
                    F.round(F.sum("total_amount"), 2).alias("revenue"),
                    F.round(F.avg("total_amount"), 2).alias("avg_ticket"),
                    F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
                )
                .orderBy(F.desc("trips"))
            )
            
            # KPI por zona
            kpi_top_pickup_zones = (
                df_clean
                .groupBy("PULocationID")
                .agg(
                    F.count("*").alias("trips"),
                    F.round(F.sum("total_amount"), 2).alias("revenue"),
                    F.round(F.avg("total_amount"), 2).alias("avg_ticket"),
                    F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
                    F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
                )
                .orderBy(F.desc("trips"))
                .limit(20)
            )
            
            # KPI calidad
            kpi_calidad = pd.DataFrame([
                {"metric": "data_source_used", "value": str(local_uri)},
                {"metric": "original_rows", "value": int(total_rows)},
                {"metric": "clean_rows", "value": int(clean_rows)},
                {"metric": "removed_rows", "value": int(removed_rows)},
                {"metric": "removed_pct", "value": round(removed_pct, 4)},
                {"metric": "quality_score", "value": round(1 - removed_pct, 4)},
            ])
            
            # Guardar KPIs en JSON
            def spark_df_to_records(spark_df):
                return spark_df.toPandas().to_dict(orient="records")
            
            kpi_payload = {
                "metadata": {
                    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                    "dataset": "NYC TLC Green Taxi - January 2025",
                    "source_url": HTTPS_URL,
                    "sample_enabled": bool(USE_SAMPLE),
                    "sample_fraction": float(SAMPLE_FRACTION) if USE_SAMPLE else None,
                },
                "kpi_resumen": spark_df_to_records(kpi_resumen),
                "kpi_por_dia": spark_df_to_records(kpi_por_dia),
                "kpi_por_hora": spark_df_to_records(kpi_por_hora),
                "kpi_tipo_pago": spark_df_to_records(kpi_tipo_pago),
                "kpi_top_pickup_zones": spark_df_to_records(kpi_top_pickup_zones),
                "kpi_calidad": kpi_calidad.to_dict(orient="records"),
            }
            
            # Guardar localmente
            with open(LOCAL_KPIS_PATH, "w", encoding="utf-8") as f:
                json.dump(kpi_payload, f, ensure_ascii=False, indent=2, default=str)
            
            # Subir a S3
            with st.spinner("Subiendo KPIs a S3..."):
                s3 = boto3.client("s3")
                s3.upload_file(
                    Filename=str(LOCAL_KPIS_PATH),
                    Bucket=S3_BUCKET,
                    Key=S3_KPIS_KEY,
                    ExtraArgs={"ContentType": "application/json"},
                )
                st.success("✅ KPIs subidos a S3")
            
            spark.stop()
            return True
            
    except Exception as e:
        st.error(f"❌ Error en procesamiento: {e}")
        import traceback
        st.code(traceback.format_exc())
        return False

# =====================================
# CONFIGURACIÓN DE STREAMLIT
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
.stApp {
    background: linear-gradient(120deg, #f1f5f9, #dbeafe);
}
.block-container {
    max-width: 1100px;
    padding-top: 2rem;
}
.navbar-custom {
    background: #0f172a;
    padding: 20px;
    border-radius: 18px;
    margin-bottom: 30px;
    box-shadow: 0 8px 20px rgba(0,0,0,.15);
}
.navbar-title {
    color: white;
    font-size: 28px;
    font-weight: 800;
}
.navbar-text {
    color: #cbd5e1;
    font-size: 14px;
}
.card-custom {
    background: white;
    border-radius: 20px;
    padding: 35px;
    box-shadow: 0 10px 30px rgba(15,23,42,.12);
}
.section-title {
    color: #0f172a;
    font-size: 22px;
    font-weight: 700;
}
label {
    color: #1e293b !important;
    font-weight: 700 !important;
}
div[data-baseweb="select"] > div {
    border-radius: 12px !important;
    border: 1px solid #94a3b8;
}
.stButton button {
    width: 100%;
    height: 52px;
    background: #2563eb;
    color: white;
    border-radius: 12px;
    border: none;
    font-weight: 700;
    font-size: 17px;
}
.stButton button:hover {
    background: #1d4ed8;
}
.stAlert {
    border-radius: 12px;
}
.metric-card {
    background: #f8fafc;
    border-radius: 12px;
    padding: 16px;
    text-align: center;
    border: 1px solid #e2e8f0;
}
.metric-value {
    font-size: 28px;
    font-weight: 700;
    color: #0f172a;
}
.metric-label {
    font-size: 14px;
    color: #64748b;
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
        Procesamiento y visualización de datos de taxis NYC
    </span>
</div>
""",
unsafe_allow_html=True
)

# =====================================
# CARDS PRINCIPALES
# =====================================
tab1, tab2 = st.tabs(["📊 Dashboard KPIs", "⚙️ Procesar Datos"])

# =====================================
# TAB 1: DASHBOARD
# =====================================
with tab1:
    st.markdown(
    """
    <div class="card-custom">
    <div class="section-title">📈 Dashboard de KPIs</div>
    <br>
    """,
    unsafe_allow_html=True
    )
    
    # Intentar cargar KPIs desde S3 o local
    kpis_loaded = False
    
    # Primero intentar cargar desde S3
    if download_kpis_from_s3():
        try:
            with open(LOCAL_KPIS_PATH, "r", encoding="utf-8") as f:
                kpis = json.load(f)
            kpis_loaded = True
        except Exception as e:
            st.warning(f"⚠️ No se pudieron cargar KPIs locales: {e}")
    
    # Si no hay KPIs, mostrar mensaje
    if not kpis_loaded:
        st.info("📊 No hay KPIs disponibles. Ve a la pestaña 'Procesar Datos' para generarlos.")
    else:
        # Mostrar KPIs
        col1, col2, col3, col4 = st.columns(4)
        
        resumen = kpis["kpi_resumen"][0]
        
        with col1:
            st.metric(
                "Total Viajes",
                f"{resumen['total_trips']:,}",
                delta=None
            )
        with col2:
            st.metric(
                "Ingresos Totales",
                f"${resumen['total_revenue']:,.2f}",
                delta=None
            )
        with col3:
            st.metric(
                "Ticket Promedio",
                f"${resumen['avg_ticket']:.2f}",
                delta=None
            )
        with col4:
            st.metric(
                "Propina Promedio",
                f"${resumen['avg_tip']:.2f}",
                delta=f"{resumen['avg_tip_pct']}%"
            )
        
        st.markdown("---")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(
                "Distancia Promedio",
                f"{resumen['avg_trip_distance']:.2f} mi"
            )
        with col2:
            st.metric(
                "Duración Promedio",
                f"{resumen['avg_duration_min']:.1f} min"
            )
        with col3:
            st.metric(
                "Pasajeros Promedio",
                f"{resumen['avg_passenger_count']:.1f}"
            )
        
        st.markdown("---")
        
        # KPIs por día - Gráfico
        st.subheader("📊 Viajes por Día")
        df_dia = pd.DataFrame(kpis["kpi_por_dia"])
        st.line_chart(df_dia.set_index("pickup_date")["trips"])
        
        # KPIs por hora
        st.subheader("📊 Viajes por Hora")
        df_hora = pd.DataFrame(kpis["kpi_por_hora"])
        st.bar_chart(df_hora.set_index("pickup_hour")["trips"])
        
        # Top zonas de recogida
        st.subheader("📍 Top 10 Zonas de Recogida")
        df_zones = pd.DataFrame(kpis["kpi_top_pickup_zones"]).head(10)
        st.dataframe(
            df_zones[["PULocationID", "trips", "revenue", "avg_ticket"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "PULocationID": "Zona ID",
                "trips": "Viajes",
                "revenue": st.column_config.NumberColumn("Ingresos", format="$%.2f"),
                "avg_ticket": st.column_config.NumberColumn("Ticket Promedio", format="$%.2f"),
            }
        )
        
        # KPIs por tipo de pago
        st.subheader("💳 Tipo de Pago")
        df_pago = pd.DataFrame(kpis["kpi_tipo_pago"])
        st.dataframe(
            df_pago[["payment_type_label", "trips", "revenue", "avg_ticket"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "payment_type_label": "Tipo de Pago",
                "trips": "Viajes",
                "revenue": st.column_config.NumberColumn("Ingresos", format="$%.2f"),
                "avg_ticket": st.column_config.NumberColumn("Ticket Promedio", format="$%.2f"),
            }
        )
        
        # Calidad de datos
        st.subheader("📊 Calidad de Datos")
        df_calidad = pd.DataFrame(kpis["kpi_calidad"])
        st.dataframe(
            df_calidad,
            use_container_width=True,
            hide_index=True,
        )
    
    st.markdown("</div>", unsafe_allow_html=True)

# =====================================
# TAB 2: PROCESAMIENTO
# =====================================
with tab2:
    st.markdown(
    """
    <div class="card-custom">
    <div class="section-title">⚙️ Procesamiento de Datos</div>
    <br>
    """,
    unsafe_allow_html=True
    )
    
    st.info("""
    **Flujo de procesamiento:**
    1. 📥 Descarga del archivo Parquet desde S3
    2. 🔄 Procesamiento con Apache Spark
    3. 📊 Cálculo de KPIs
    4. ☁️ Subida de resultados a S3
    """)
    
    # Período
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
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        mes_nombre = st.selectbox(
            "📅 Mes",
            list(meses.keys()),
            index=0,
            key="mes_procesamiento"
        )
    
    with col2:
        anio = st.selectbox(
            "📅 Año",
            range(2024, 2031),
            index=list(range(2024, 2031)).index(2025),
            key="anio_procesamiento"
        )
    
    with col3:
        colores = {"Amarillo": "yellow", "Verde": "green"}
        color_seleccionado = st.selectbox(
            "🚕 Color de taxi",
            list(colores.keys()),
            key="color_procesamiento"
        )
        color_taxi = colores[color_seleccionado]
    
    st.write("")
    
    # Botón de procesamiento
    if st.button("🚀 Ejecutar Procesamiento"):
        
        # Paso 1: Descargar Parquet
        with st.status("📥 Descargando archivo Parquet...", expanded=True) as status:
            if download_parquet():
                status.update(label="✅ Parquet descargado", state="complete")
            else:
                status.update(label="❌ Error al descargar Parquet", state="error")
                st.stop()
        
        # Paso 2: Procesar con Spark
        with st.status("⚙️ Procesando con Spark...", expanded=True) as status:
            if process_with_spark():
                status.update(label="✅ Procesamiento completado", state="complete")
                st.success("🎉 KPIs generados y subidos a S3 correctamente!")
                st.balloons()
                
                # Recargar KPIs
                with open(LOCAL_KPIS_PATH, "r", encoding="utf-8") as f:
                    kpis = json.load(f)
                
                # Mostrar resumen rápido
                st.subheader("📊 Resumen de KPIs generados")
                resumen = kpis["kpi_resumen"][0]
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Viajes", f"{resumen['total_trips']:,}")
                with col2:
                    st.metric("Ingresos Totales", f"${resumen['total_revenue']:,.2f}")
                with col3:
                    st.metric("Ticket Promedio", f"${resumen['avg_ticket']:.2f}")
                with col4:
                    st.metric("Calidad", f"{resumen.get('quality_score', 0):.1%}")
            else:
                status.update(label="❌ Error en el procesamiento", state="error")
    
    st.markdown("</div>", unsafe_allow_html=True)

# =====================================
# FOOTER
# =====================================
st.markdown(
"""
<div style="
    margin-top: 30px;
    padding: 15px;
    text-align: center;
    color: #64748b;
    font-size: 12px;
    border-top: 1px solid #e2e8f0;
">
    🚕 Taxi Analytics Platform v2.0 | AWS + Streamlit
</div>
""",
unsafe_allow_html=True
)