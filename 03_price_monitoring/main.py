"""
Cloud Function — Procesador de Actualizaciones de Precio (Gen 2)
=================================================================

Trigger: Pub/Sub topic "price-updates"

Flujo:
  1. Recibe un mensaje de Pub/Sub con datos base64.
  2. Decodifica y parsea el JSON.
  3. Valida que tenga los campos requeridos.
  4. Inserta la actualización en BigQuery (price_history).

Manejo de errores:
  - JSON inválido o campos faltantes → log + discard (no reintentar).
  - Error de BigQuery → relanza excepción (para retry automático).

Variables de entorno requeridas:
  - GCP_PROJECT:  ID del proyecto de GCP.
  - DATASET_NAME: Nombre del dataset de BigQuery (default: dropshipping).
"""

import base64
import json
import logging
import os

import functions_framework
from cloudevents.http import CloudEvent
from google.cloud import bigquery

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GCP_PROJECT = os.environ.get("GCP_PROJECT", "")
DATASET_NAME = os.environ.get("DATASET_NAME", "dropshipping")

# Campos requeridos en el mensaje de precio.
REQUIRED_FIELDS = {"product_id", "old_price", "new_price", "change_percent", "timestamp"}


# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================


def decode_pubsub_message(cloud_event: CloudEvent) -> dict | None:
    """Decodifica el mensaje base64 de Pub/Sub y parsea el JSON.

    Los mensajes de Pub/Sub llegan con el payload codificado en base64
    dentro de cloud_event.data["message"]["data"]. Esta función:
      1. Decodifica el base64.
      2. Parsea el JSON resultante.

    Args:
        cloud_event: Evento de CloudEvents con la data de Pub/Sub.

    Returns:
        Dict con el mensaje parseado, o None si falla la decodificación.
    """
    try:
        raw_data = cloud_event.data["message"]["data"]
        decoded = base64.b64decode(raw_data).decode("utf-8")
        return json.loads(decoded)
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        logger.error("Error decodificando mensaje Pub/Sub: %s", str(e))
        return None
    except Exception as e:
        logger.error("Error inesperado decodificando mensaje: %s", str(e))
        return None


def validate_message(message: dict) -> bool:
    """Valida que el mensaje tenga todos los campos requeridos.

    Args:
        message: Dict con los datos del mensaje de precio.

    Returns:
        True si todos los campos requeridos están presentes.
    """
    missing = REQUIRED_FIELDS - set(message.keys())
    if missing:
        logger.error("Campos faltantes en mensaje: %s", missing)
        return False
    return True


def insert_to_bigquery(message: dict) -> None:
    """Inserta una actualización de precio en la tabla price_history.

    Mapea los campos del mensaje Pub/Sub al schema de BigQuery:
      - product_id → product_id (STRING)
      - old_price → old_price (FLOAT)
      - new_price → new_price (FLOAT)
      - change_percent → change_percent (FLOAT)
      - timestamp → updated_at (TIMESTAMP)

    Args:
        message: Dict con los datos validados del mensaje.

    Raises:
        RuntimeError: Si BigQuery reporta errores en el insert.
    """
    client = bigquery.Client(project=GCP_PROJECT)
    table_id = f"{GCP_PROJECT}.{DATASET_NAME}.price_history"

    row = {
        "product_id": message["product_id"],
        "old_price": float(message["old_price"]),
        "new_price": float(message["new_price"]),
        "change_percent": float(message["change_percent"]),
        "updated_at": message["timestamp"],
    }

    errors = client.insert_rows_json(table_id, [row])

    if errors:
        logger.error("Error insertando en BigQuery: %s", errors)
        raise RuntimeError(f"BigQuery streaming insert falló: {errors}")

    logger.info(
        "✅ Precio registrado: %s → $%.2f (%+.1f%%)",
        message["product_id"],
        message["new_price"],
        message["change_percent"],
    )


# =============================================================================
# ENTRY POINT — CLOUD FUNCTION (GEN 2)
# =============================================================================


@functions_framework.cloud_event
def process_price_update(cloud_event: CloudEvent) -> None:
    """Punto de entrada de la Cloud Function.

    Se ejecuta cada vez que llega un mensaje al tópico price-updates.

    Flujo:
      1. Decodifica el mensaje base64 de Pub/Sub.
      2. Valida los campos requeridos.
      3. Inserta en BigQuery.

    Errores de formato → discard (log + return sin excepción).
    Errores de BigQuery → relanza para retry automático.

    Args:
        cloud_event: Evento de CloudEvents con el mensaje de Pub/Sub.
    """
    # 1. Decodificar mensaje.
    message = decode_pubsub_message(cloud_event)
    if message is None:
        logger.error("Mensaje descartado: no se pudo decodificar.")
        return

    # 2. Validar campos.
    if not validate_message(message):
        logger.error("Mensaje descartado: campos inválidos.")
        return

    # 3. Insertar en BigQuery (errores se relanzan para retry).
    insert_to_bigquery(message)
