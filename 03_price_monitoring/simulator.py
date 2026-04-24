"""
Simulador de Monitoreo de Precios en Tiempo Real
=================================================

Módulo 03 del pipeline de Resparked Data Intelligence.

Lee productos de final_products al inicio, y en un loop infinito
cada 5 segundos elige un producto al azar, genera un nuevo precio
con un factor aleatorio (0.8–1.2), y publica la actualización
a Pub/Sub para que la Cloud Function la persista en BigQuery.

Si el cambio de precio supera ±10%, se loguea como WARNING (alerta).

Variables de entorno requeridas:
  - GCP_PROJECT:  ID del proyecto de GCP.
  - DATASET_NAME: Nombre del dataset de BigQuery (default: dropshipping).

Uso:
  export GCP_PROJECT=tu-proyecto
  python simulator.py
"""

import json
import logging
import os
import random
import time
from datetime import datetime, timezone

from google.cloud import bigquery
from google.cloud import pubsub_v1

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

GCP_PROJECT = os.environ.get("GCP_PROJECT", "")
DATASET_NAME = os.environ.get("DATASET_NAME", "dropshipping")

# Nombre del tópico de Pub/Sub — debe coincidir con infra/setup.sh.
PUBSUB_TOPIC = "price-updates"

# Intervalo entre actualizaciones de precio (segundos).
UPDATE_INTERVAL = 5

# Factor de variación de precio: entre 0.8 (−20%) y 1.2 (+20%).
PRICE_FACTOR_MIN = 0.8
PRICE_FACTOR_MAX = 1.2

# Umbral de alerta: cambios mayores a este porcentaje se loguean como WARNING.
ALERT_THRESHOLD = 10.0


# =============================================================================
# FUNCIONES DE NEGOCIO
# =============================================================================


def load_products() -> dict[str, float]:
    """Lee product_id y price de final_products en BigQuery.

    Retorna un diccionario {product_id: price} para mantener
    el estado actual de los precios en memoria durante la simulación.

    Returns:
        Dict con product_id como key y price como value.
    """
    client = bigquery.Client(project=GCP_PROJECT)

    query = f"""
    SELECT product_id, price
    FROM `{GCP_PROJECT}.{DATASET_NAME}.final_products`
    """

    logger.info("Cargando productos desde %s.final_products...", DATASET_NAME)
    df = client.query(query).to_dataframe()

    if df.empty:
        logger.warning("No se encontraron productos en final_products.")
        return {}

    # Convertimos a dict para acceso O(1) durante la simulación.
    products = dict(zip(df["product_id"], df["price"]))
    logger.info("Productos cargados: %d", len(products))
    return products


def generate_new_price(old_price: float) -> float:
    """Genera un nuevo precio aplicando un factor aleatorio.

    El factor varía uniformemente entre PRICE_FACTOR_MIN y PRICE_FACTOR_MAX,
    lo que produce cambios de −20% a +20% sobre el precio actual.

    Args:
        old_price: Precio actual del producto.

    Returns:
        Nuevo precio redondeado a 2 decimales.
    """
    factor = random.uniform(PRICE_FACTOR_MIN, PRICE_FACTOR_MAX)
    return round(old_price * factor, 2)


def calculate_change_percent(old_price: float, new_price: float) -> float:
    """Calcula el porcentaje de cambio entre dos precios.

    Args:
        old_price: Precio anterior.
        new_price: Precio nuevo.

    Returns:
        Porcentaje de cambio (positivo = subió, negativo = bajó).
    """
    return round(((new_price - old_price) / old_price) * 100, 2)


def is_alert(change_percent: float) -> bool:
    """Determina si un cambio de precio es una alerta.

    Un cambio es alerta si supera ESTRICTAMENTE el umbral de ±10%.

    Args:
        change_percent: Porcentaje de cambio del precio.

    Returns:
        True si abs(change_percent) > ALERT_THRESHOLD.
    """
    return abs(change_percent) > ALERT_THRESHOLD


def publish_price_update(
    product_id: str,
    old_price: float,
    new_price: float,
    change_percent: float,
) -> None:
    """Publica una actualización de precio a Pub/Sub.

    El mensaje se publica como JSON serializado en bytes UTF-8.
    Incluye un timestamp ISO8601 del momento de publicación.

    Args:
        product_id: ID del producto que cambió de precio.
        old_price: Precio anterior.
        new_price: Precio nuevo.
        change_percent: Porcentaje de cambio.
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GCP_PROJECT, PUBSUB_TOPIC)

    message = {
        "product_id": product_id,
        "old_price": old_price,
        "new_price": new_price,
        "change_percent": change_percent,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    data = json.dumps(message).encode("utf-8")
    future = publisher.publish(topic_path, data=data)
    future.result()  # Espera confirmación de Pub/Sub.

    logger.info(
        "📤 Publicado: %s → $%.2f (cambio: %+.1f%%)",
        product_id,
        new_price,
        change_percent,
    )


# =============================================================================
# LOOP PRINCIPAL
# =============================================================================


def run_simulation() -> None:
    """Ejecuta el loop infinito de simulación de precios.

    Cada UPDATE_INTERVAL segundos:
      1. Elige un producto al azar.
      2. Genera un nuevo precio con factor aleatorio.
      3. Calcula el porcentaje de cambio.
      4. Si el cambio supera ±10% → loguea WARNING.
      5. Publica la actualización a Pub/Sub.
      6. Actualiza el precio en memoria.
    """
    if not GCP_PROJECT:
        raise ValueError(
            "GCP_PROJECT no está configurado. "
            "Exportalo con: export GCP_PROJECT=tu-proyecto"
        )

    # Carga inicial de productos.
    products = load_products()

    if not products:
        logger.error("No hay productos para simular. Abortando.")
        return

    logger.info("━" * 50)
    logger.info("SIMULADOR DE PRECIOS INICIADO")
    logger.info("━" * 50)
    logger.info("Productos monitoreados: %d", len(products))
    logger.info("Intervalo: cada %d segundos", UPDATE_INTERVAL)
    logger.info("Umbral de alerta: ±%.0f%%", ALERT_THRESHOLD)
    logger.info("━" * 50)

    try:
        while True:
            # 1. Elegir producto al azar.
            product_id = random.choice(list(products.keys()))
            old_price = products[product_id]

            # 2. Generar nuevo precio.
            new_price = generate_new_price(old_price)

            # 3. Calcular cambio.
            change_percent = calculate_change_percent(old_price, new_price)

            # 4. Loguear según severidad.
            if is_alert(change_percent):
                logger.warning(
                    "⚠️ ALERTA: %s cambió %+.1f%% → $%.2f",
                    product_id,
                    change_percent,
                    new_price,
                )
            else:
                logger.info(
                    "📊 %s: $%.2f → $%.2f (%+.1f%%)",
                    product_id,
                    old_price,
                    new_price,
                    change_percent,
                )

            # 5. Publicar a Pub/Sub.
            publish_price_update(product_id, old_price, new_price, change_percent)

            # 6. Actualizar precio en memoria para la próxima iteración.
            products[product_id] = new_price

            # Esperar antes de la siguiente iteración.
            time.sleep(UPDATE_INTERVAL)

    except KeyboardInterrupt:
        logger.info("Simulador detenido por el usuario.")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    run_simulation()
