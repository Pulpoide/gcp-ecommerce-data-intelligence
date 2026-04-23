"""
Cloud Function — Ingesta Batch de Productos (Gen 2)
====================================================

Trigger: google.cloud.storage.object.v1.finalized
Bucket:  {project-b8b7f8b6-4ca7-4b6e-96b}-raw-products

Flujo:
  1. Se sube un CSV al bucket raw-products.
  2. Eventarc dispara esta función.
  3. Se lee el CSV, se valida fila por fila.
  4. Filas válidas → BigQuery (staging_products).
  5. Si el CSV es ilegible o le faltan columnas → se mueve a failed-products.

Variables de entorno requeridas:
  - GCP_PROJECT:  ID del proyecto de GCP.
  - DATASET_NAME: Nombre del dataset de BigQuery (default: dropshipping).
"""

import io
import logging
import os
from datetime import datetime, timezone

import functions_framework
import pandas as pd
from cloudevents.http import CloudEvent
from google.cloud import bigquery, storage

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

# Nivel de logging — Cloud Logging captura INFO y superiores automáticamente.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Variables de entorno con defaults sensatos.
GCP_PROJECT = os.environ.get("GCP_PROJECT", "")
DATASET_NAME = os.environ.get("DATASET_NAME", "dropshipping")

# Columnas esperadas en el CSV de Shopify.
# Si el CSV no tiene TODAS estas columnas, se rechaza completo.
EXPECTED_COLUMNS = {
    "product_id",
    "name",
    "description",
    "price",
    "supplier",
    "stock",
    "created_at",
}

# Schema explícito para el job de carga a BigQuery.
# Debe coincidir EXACTAMENTE con la tabla staging_products definida en infra/setup.sh.
BQ_SCHEMA = [
    bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("price", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("supplier", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("stock", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="NULLABLE"),
]


# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================


def validate_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Valida cada fila del DataFrame y separa válidas de inválidas.

    Reglas de validación (row-level):
      - product_id: no puede estar vacío ni ser NaN.
      - price: debe ser numérico y estrictamente mayor a 0.
      - supplier: no puede estar vacío, ser NaN, ni ser "Unknown".

    Args:
        df: DataFrame con los datos crudos del CSV.

    Returns:
        Tupla (df_valid, df_invalid) con las filas separadas.
    """
    # Inicializamos una columna de motivos de rechazo.
    # Si al final está vacía, la fila es válida.
    df = df.copy()
    df["_rejection_reason"] = ""

    # --- Regla 1: product_id no puede estar vacío ---
    mask_no_id = df["product_id"].isna() | (
        df["product_id"].astype(str).str.strip() == ""
    )
    df.loc[mask_no_id, "_rejection_reason"] += "product_id vacío; "

    # --- Regla 2: price debe ser numérico > 0 ---
    # Convertimos a numérico forzando errores a NaN.
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    mask_bad_price = df["price"].isna() | (df["price"] <= 0)
    df.loc[mask_bad_price, "_rejection_reason"] += (
        "price inválido (<=0 o no numérico); "
    )

    # --- Regla 3: supplier no puede ser vacío ni "Unknown" ---
    mask_bad_supplier = (
        df["supplier"].isna()
        | (df["supplier"].astype(str).str.strip() == "")
        | (df["supplier"].astype(str).str.strip() == "Unknown")
    )
    df.loc[mask_bad_supplier, "_rejection_reason"] += "supplier vacío o Unknown; "

    # Separamos válidas de inválidas.
    is_invalid = df["_rejection_reason"].str.strip() != ""
    df_invalid = df[is_invalid].copy()
    df_valid = df[~is_invalid].copy()

    # Logueamos detalle de cada motivo de rechazo.
    if not df_invalid.empty:
        rejection_counts = {}
        for reason_str in df_invalid["_rejection_reason"]:
            for reason in reason_str.split("; "):
                reason = reason.strip()
                if reason:
                    rejection_counts[reason] = rejection_counts.get(reason, 0) + 1

        logger.info("Filas descartadas: %d de %d total", len(df_invalid), len(df))
        for reason, count in rejection_counts.items():
            logger.info("  → Motivo: '%s' — %d filas", reason, count)
    else:
        logger.info("Todas las filas pasaron la validación (%d filas)", len(df))

    # Eliminamos la columna auxiliar antes de retornar.
    df_valid.drop(columns=["_rejection_reason"], inplace=True)
    df_invalid.drop(columns=["_rejection_reason"], inplace=True)

    return df_valid, df_invalid


def load_to_bigquery(df: pd.DataFrame, table_id: str) -> None:
    """Carga un DataFrame validado en BigQuery staging_products.

    Usa un schema explícito en el job_config para garantizar que los tipos
    coincidan con la definición de la tabla en BQ. Esto previene errores
    silenciosos de casteo automático.

    Si la carga falla por un error de red, cuotas o permisos, la excepción
    se relanza para que Cloud Functions reintente (si está configurado).

    Args:
        df: DataFrame con filas válidas listas para cargar.
        table_id: ID completo de la tabla (project.dataset.table).

    Raises:
        google.api_core.exceptions.GoogleAPIError: Si la carga falla.
    """
    client = bigquery.Client(project=GCP_PROJECT)

    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # No usamos autodetect — el schema está definido explícitamente
        # para evitar sorpresas con tipos inferidos.
    )

    logger.info("Cargando %d filas válidas en %s...", len(df), table_id)

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Bloquea hasta que termine. Lanza excepción si falla.
        logger.info(
            "Carga exitosa: %d filas insertadas en %s", job.output_rows, table_id
        )
    except Exception as e:
        logger.error("Error cargando datos en BigQuery: %s", str(e))
        # Relanzamos para que Cloud Functions pueda reintentar.
        # Si el error es transitorio (red, cuotas), el retry lo resuelve.
        # Si es permanente (schema mismatch), el retry también fallará
        # y quedará registrado en Cloud Logging para debugging.
        raise


def move_blob_to_failed(
    source_bucket_name: str,
    blob_name: str,
    destination_bucket_name: str,
) -> None:
    """Mueve un blob de un bucket a otro (para archivos rechazados).

    El archivo se copia al bucket de destino y luego se elimina del origen.
    Si el destino ya tiene un archivo con el mismo nombre, se sobreescribe.

    Args:
        source_bucket_name: Bucket de origen (raw-products).
        blob_name: Nombre/path del archivo dentro del bucket.
        destination_bucket_name: Bucket de destino (failed-products).
    """
    client = storage.Client(project=GCP_PROJECT)

    source_bucket = client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = client.bucket(destination_bucket_name)

    # Copiamos al bucket de fallidos.
    source_bucket.copy_blob(source_blob, destination_bucket, blob_name)
    # Eliminamos del bucket original.
    source_blob.delete()

    logger.info(
        "Archivo movido: gs://%s/%s → gs://%s/%s",
        source_bucket_name,
        blob_name,
        destination_bucket_name,
        blob_name,
    )


def _read_csv_from_gcs(bucket_name: str, blob_name: str) -> pd.DataFrame:
    """Lee un CSV desde GCS intentando múltiples encodings.

    Intenta primero con UTF-8 (lo más común). Si falla, cae a latin-1
    que acepta cualquier byte y nunca falla — pero puede producir
    caracteres incorrectos en casos extremos.

    Args:
        bucket_name: Nombre del bucket de GCS.
        blob_name: Path del archivo dentro del bucket.

    Returns:
        DataFrame con el contenido del CSV.

    Raises:
        ValueError: Si el CSV no se puede parsear con ningún encoding.
        pd.errors.ParserError: Si el CSV tiene formato inválido.
    """
    client = storage.Client(project=GCP_PROJECT)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    raw_bytes = blob.download_as_bytes()

    # Intentamos UTF-8 primero (encoding estándar).
    for encoding in ["utf-8", "latin-1"]:
        try:
            df = pd.read_csv(
                io.BytesIO(raw_bytes),
                encoding=encoding,
                dtype=str,  # Leemos todo como string para validar después.
            )
            logger.info(
                "CSV leído exitosamente con encoding '%s': %d filas, %d columnas",
                encoding,
                len(df),
                len(df.columns),
            )
            return df
        except UnicodeDecodeError:
            logger.warning("Encoding '%s' falló, probando siguiente...", encoding)
            continue

    raise ValueError(
        f"No se pudo decodificar el CSV gs://{bucket_name}/{blob_name} "
        "con ningún encoding soportado (utf-8, latin-1)"
    )


# =============================================================================
# ENTRY POINT — CLOUD FUNCTION (GEN 2)
# =============================================================================


@functions_framework.cloud_event
def process_csv(cloud_event: CloudEvent) -> None:
    """Punto de entrada de la Cloud Function.

    Se ejecuta cada vez que se sube (finaliza) un archivo al bucket
    raw-products. Solo procesa archivos con extensión .csv.

    Flujo:
      1. Extrae metadata del evento (bucket, nombre del archivo).
      2. Ignora archivos que no sean .csv.
      3. Lee el CSV desde GCS.
      4. Valida que tenga las columnas esperadas.
      5. Valida cada fila individualmente.
      6. Carga filas válidas en BigQuery.
      7. Si el CSV es ilegible → lo mueve a failed-products.

    Args:
        cloud_event: Evento de CloudEvents con la metadata del objeto de GCS.
    """
    # --- Extraer metadata del evento ---
    data = cloud_event.data
    bucket_name = data["bucket"]
    blob_name = data["name"]
    failed_bucket = f"{GCP_PROJECT}-failed-products"

    logger.info(
        "Evento recibido: archivo '%s' en bucket '%s'",
        blob_name,
        bucket_name,
    )

    # --- Filtrar archivos que no sean CSV ---
    if not blob_name.lower().endswith(".csv"):
        logger.info("Archivo '%s' ignorado (no es .csv)", blob_name)
        return

    # --- Leer el CSV desde GCS ---
    try:
        df = _read_csv_from_gcs(bucket_name, blob_name)
    except (ValueError, pd.errors.ParserError) as e:
        logger.error("CSV ilegible, moviendo a failed-products: %s", str(e))
        move_blob_to_failed(bucket_name, blob_name, failed_bucket)
        return

    # --- Validar columnas del CSV ---
    # Normalizamos nombres de columna (strip de espacios).
    df.columns = df.columns.str.strip()
    actual_columns = set(df.columns)
    missing_columns = EXPECTED_COLUMNS - actual_columns

    if missing_columns:
        logger.error(
            "CSV rechazado — columnas faltantes: %s. Moviendo a failed-products.",
            missing_columns,
        )
        move_blob_to_failed(bucket_name, blob_name, failed_bucket)
        return

    # --- Validar filas ---
    df_valid, df_invalid = validate_dataframe(df)

    if df_valid.empty:
        logger.warning(
            "Todas las filas fueron descartadas (%d). "
            "No hay datos para cargar en BigQuery.",
            len(df_invalid),
        )
        return

    # --- Preparar datos para BigQuery ---
    # Agregamos ingested_at con el timestamp actual en UTC.
    df_valid["ingested_at"] = datetime.now(timezone.utc).isoformat()

    # Casteamos tipos para que coincidan con el schema de BQ.
    # price y stock ya fueron validados, pero necesitamos el tipo correcto.
    df_valid["price"] = pd.to_numeric(df_valid["price"], errors="coerce")
    df_valid["stock"] = pd.to_numeric(
        df_valid["stock"], errors="coerce", downcast="integer"
    )

    # Seleccionamos solo las columnas que van a BQ (en el orden del schema).
    # Excluimos created_at porque no es parte del schema de staging_products.
    bq_columns = [
        "product_id",
        "name",
        "description",
        "price",
        "supplier",
        "stock",
        "ingested_at",
    ]
    df_valid = df_valid[bq_columns]

    # --- Cargar en BigQuery ---
    table_id = f"{GCP_PROJECT}.{DATASET_NAME}.staging_products"
    load_to_bigquery(df_valid, table_id)

    logger.info(
        "Procesamiento completo: %d válidas cargadas, %d descartadas de '%s'",
        len(df_valid),
        len(df_invalid),
        blob_name,
    )
