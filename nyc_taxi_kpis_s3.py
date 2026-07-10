from pathlib import Path
import json
from datetime import datetime, timezone

import boto3
import pandas as pd
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import NumericType

AWS_ACCESS_KEY="TU_ACCESS_KEY"
AWS_SECRET_KEY="TU_SECRET_KEY"
AWS_REGION="us-east-1"
S3_BUCKET_DATA="TU_BUCKET"
S3_PREFIX="nyc_taxi/parquet/"

USE_SAMPLE = True
SAMPLE_FRACTION = 0.10
RANDOM_SEED = 42

KPI_DIR = Path("kpis")
KPI_DIR.mkdir(parents=True, exist_ok=True)
KPI_JSON_PATH = KPI_DIR / "nyc_taxi_kpis.json"

# Opcional: subir el JSON a S3.
# Para clase, pueden dejarlo en False y usar el JSON local.
ENABLE_S3_UPLOAD = False
S3_BUCKET = "TU_BUCKET_AQUI"
S3_KEY = "nyc_taxi/kpis/nyc_taxi_kpis.json"

print("Parquet local:", LOCAL_PARQUET)
print("JSON de KPIs:", KPI_JSON_PATH)





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
    .appName("nyc-taxi-audit-cleaning-kpis")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .config("spark.sql.warehouse.dir", f"file://{HOME / 'spark-warehouse'}")
    .config("spark.local.dir", str(SPARK_TMP))
    .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={JAVA_TMP}")
    .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={JAVA_TMP}")
    .config("spark.sql.shuffle.partitions","8")
    .config("spark.hadoop.fs.s3a.access.key",AWS_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",AWS_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.endpoint","s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark version:", spark.version)
print("Spark UI:", spark.sparkContext.uiWebUrl)


S3_PATH=f"s3a://{S3_BUCKET_DATA}/{S3_PREFIX}"
df_raw=spark.read.parquet(S3_PATH)
DATA_SOURCE_USED=S3_PATH

if USE_SAMPLE:
    df = df_raw.sample(withReplacement=False, fraction=SAMPLE_FRACTION, seed=RANDOM_SEED)
    print(f"Usando muestra: {SAMPLE_FRACTION:.0%}")
else:
    df = df_raw

print("Fuente usada:", DATA_SOURCE_USED)


print("Columnas:", len(df.columns))
print(df.columns)

df.printSchema()
df.show(5, truncate=False)

total_rows = df.count()
total_cols = len(df.columns)

print("Filas:", total_rows)
print("Columnas:", total_cols)

null_exprs = [
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df.columns
]

null_counts_row = df.select(null_exprs).collect()[0].asDict()

nulls_df = spark.createDataFrame(
    [
        (col_name, int(null_count), float(null_count) / total_rows if total_rows else 0.0)
        for col_name, null_count in null_counts_row.items()
    ],
    ["column", "null_count", "null_pct"]
).orderBy(F.desc("null_count"))

nulls_df.show(50, truncate=False)

numeric_cols = [
    field.name
    for field in df.schema.fields
    if isinstance(field.dataType, NumericType)
]

print("Columnas numéricas:")
print(numeric_cols)

df.select(numeric_cols).describe().show(truncate=False)

df_audit = (
    df
    .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
    .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
    .withColumn(
        "duration_min",
        (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60
    )
    .withColumn("pickup_date", F.to_date("pickup_ts"))
    .withColumn("pickup_hour", F.hour("pickup_ts"))
)

df_audit.select(
    "pickup_ts",
    "dropoff_ts",
    "duration_min",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount"
).show(10, truncate=False)

rules = {
    "missing_pickup_ts": F.col("pickup_ts").isNull(),
    "missing_dropoff_ts": F.col("dropoff_ts").isNull(),
    "dropoff_before_or_equal_pickup": F.col("dropoff_ts") <= F.col("pickup_ts"),
    "duration_min_lte_0": F.col("duration_min") <= 0,
    "duration_min_gt_24h": F.col("duration_min") > 24 * 60,
    "trip_distance_lte_0": F.col("trip_distance") <= 0,
    "trip_distance_gt_100": F.col("trip_distance") > 100,
    "passenger_count_null": F.col("passenger_count").isNull(),
    "passenger_count_out_of_range": ~F.col("passenger_count").between(1, 6),
    "fare_amount_negative": F.col("fare_amount") < 0,
    "total_amount_negative": F.col("total_amount") < 0,
    "pickup_outside_2024_01": ~(
        (F.col("pickup_ts") >= F.lit("2024-01-01")) &
        (F.col("pickup_ts") < F.lit("2024-02-01"))
    )
}

audit_exprs = [
    F.sum(F.when(condition, 1).otherwise(0)).alias(rule_name)
    for rule_name, condition in rules.items()
]

audit_counts = df_audit.agg(*audit_exprs).collect()[0].asDict()

audit_df = spark.createDataFrame(
    [
        (
            rule_name,
            int(count),
            float(count) / total_rows if total_rows else 0.0
        )
        for rule_name, count in audit_counts.items()
    ],
    ["rule", "bad_rows", "bad_pct"]
).orderBy(F.desc("bad_rows"))

audit_df.show(50, truncate=False)

distinct_rows = df.dropDuplicates().count()
duplicate_rows = total_rows - distinct_rows

duplicate_summary = spark.createDataFrame(
    [
        ("total_rows", total_rows),
        ("distinct_rows", distinct_rows),
        ("duplicate_rows", duplicate_rows)
    ],
    ["metric", "value"]
)

duplicate_summary.show()

df_clean = (
    df_audit
    .filter(F.col("pickup_ts").isNotNull())
    .filter(F.col("dropoff_ts").isNotNull())
    .filter(F.col("dropoff_ts") > F.col("pickup_ts"))
    .filter((F.col("pickup_ts") >= F.lit("2024-01-01")) & (F.col("pickup_ts") < F.lit("2024-02-01")))
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

print("Filas originales:", total_rows)
print("Filas limpias:", clean_rows)
print("Filas removidas:", removed_rows)
print("Porcentaje removido:", f"{removed_pct:.2%}")

post_total_rows = df_clean.count()

post_audit_exprs = [
    F.sum(F.when(condition, 1).otherwise(0)).alias(rule_name)
    for rule_name, condition in rules.items()
]

post_audit_counts = df_clean.agg(*post_audit_exprs).collect()[0].asDict()

post_audit_df = spark.createDataFrame(
    [
        (
            rule_name,
            int(count),
            float(count) / post_total_rows if post_total_rows else 0.0
        )
        for rule_name, count in post_audit_counts.items()
    ],
    ["rule", "bad_rows_after_cleaning", "bad_pct_after_cleaning"]
).orderBy(F.desc("bad_rows_after_cleaning"))

post_audit_df.show(50, truncate=False)

trips_by_hour = (
    df_clean
    .groupBy("pickup_hour")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
        F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
        F.round(F.avg("total_amount"), 2).alias("avg_total_amount")
    )
    .orderBy("pickup_hour")
)

trips_by_hour.show(24, truncate=False)

OUTPUT_PATH = Path.home() / "data" / "nyc_taxi" / "clean" / "yellow_tripdata_2024_01_clean"
OUTPUT_URI = f"file://{OUTPUT_PATH}"

(
    df_clean
    .write
    .mode("overwrite")
    .parquet(OUTPUT_URI)
)

print("Dataset limpio guardado en:")
print(OUTPUT_URI)

df_clean_reloaded = spark.read.parquet(OUTPUT_URI)

print("Filas recargadas:", df_clean_reloaded.count())
df_clean_reloaded.printSchema()
df_clean_reloaded.show(5, truncate=False)

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

kpi_resumen.show(truncate=False)


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

kpi_por_dia.show(31, truncate=False)


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

kpi_por_hora.show(24, truncate=False)


kpi_tipo_pago = (
    df_clean
    .withColumn(
        "payment_type_label",
        F.when(F.col("payment_type") == 1, "Credit card")
         .when(F.col("payment_type") == 2, "Cash")
         .when(F.col("payment_type") == 3, "No charge")
         .when(F.col("payment_type") == 4, "Dispute")
         .when(F.col("payment_type") == 5, "Unknown")
         .when(F.col("payment_type") == 6, "Voided trip")
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

kpi_tipo_pago.show(truncate=False)


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

kpi_top_pickup_zones.show(20, truncate=False)


clean_rows = int(post_total_rows)
removed_rows = int(total_rows - clean_rows)
removed_pct = float(removed_rows / total_rows) if total_rows else 0.0
quality_score = float(1 - removed_pct) if total_rows else 0.0

kpi_calidad = pd.DataFrame([
    {"metric": "data_source_used", "value": str(DATA_SOURCE_USED)},
    {"metric": "original_rows", "value": int(total_rows)},
    {"metric": "clean_rows", "value": clean_rows},
    {"metric": "removed_rows", "value": removed_rows},
    {"metric": "removed_pct", "value": round(removed_pct, 4)},
    {"metric": "quality_score", "value": round(quality_score, 4)},
    {"metric": "duplicate_rows", "value": int(duplicate_rows)},
])

kpi_calidad


def spark_df_to_records(spark_df):
    # Convierte un Spark DataFrame pequeño a lista de diccionarios.
    return spark_df.toPandas().to_dict(orient="records")

kpi_payload = {
    "metadata": {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset": "NYC TLC Yellow Taxi - January 2024",
        "source_url": HTTPS_URL,
        "sample_enabled": bool(USE_SAMPLE),
        "sample_fraction": float(SAMPLE_FRACTION) if USE_SAMPLE else None,
        "storage_format": "json",
        "intended_consumer": "streamlit",
    },
    "kpi_resumen": spark_df_to_records(kpi_resumen),
    "kpi_por_dia": spark_df_to_records(kpi_por_dia),
    "kpi_por_hora": spark_df_to_records(kpi_por_hora),
    "kpi_tipo_pago": spark_df_to_records(kpi_tipo_pago),
    "kpi_top_pickup_zones": spark_df_to_records(kpi_top_pickup_zones),
    "kpi_calidad": kpi_calidad.to_dict(orient="records"),
}

with open(KPI_JSON_PATH, "w", encoding="utf-8") as f:
    json.dump(kpi_payload, f, ensure_ascii=False, indent=2, default=str)

print("JSON de KPIs guardado en:")
print(KPI_JSON_PATH.resolve())


with open(KPI_JSON_PATH, "r", encoding="utf-8") as f:
    loaded_kpis = json.load(f)

print("Secciones del JSON:")
print(list(loaded_kpis.keys()))

print("KPIs resumen:")
loaded_kpis["kpi_resumen"][0]


ENABLE_S3_UPLOAD = True
S3_BUCKET = "xideralaws-curso-benjamin2026"
S3_KEY = "nyc_taxi/kpis/nyc_taxi_kpis.json"

if ENABLE_S3_UPLOAD:
    s3 = boto3.client("s3")
    s3.upload_file(
        Filename=str(KPI_JSON_PATH),
        Bucket=S3_BUCKET,
        Key=S3_KEY,
        ExtraArgs={"ContentType": "application/json"},
    )
    print("JSON subido a S3:")
    print(f"s3://{S3_BUCKET}/{S3_KEY}")
else:
    print("ENABLE_S3_UPLOAD está en False. Se generó solo el JSON local.")
    print("Cuando quieran probar S3, cambien ENABLE_S3_UPLOAD a True y configuren bucket/key.")


# Ejecuta esta celda cuando ya hayas terminado.
# spark.stop()


