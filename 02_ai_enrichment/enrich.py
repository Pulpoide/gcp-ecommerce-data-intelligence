"""
Script de Enriquecimiento de Productos con Gemini AI
=====================================================

Módulo 02 del pipeline de Resparked Data Intelligence.

Lee productos de final_products que aún no fueron enriquecidos,
los envía a Gemini para clasificación automática,
y escribe los resultados en enriched_products.

El procesamiento es INCREMENTAL: solo enriquece productos que no
tienen entrada en enriched_products (LEFT JOIN + IS NULL).

Variables de entorno requeridas:
  - GCP_PROJECT:    ID del proyecto de GCP.
  - DATASET_NAME:   Nombre del dataset de BigQuery (default: dropshipping).

Prerequisitos:
  - API de Vertex AI habilitada: gcloud services enable aiplatform.googleapis.com
  - Autenticación via ADC (Application Default Credentials).

Uso:
  export GCP_PROJECT=tu-proyecto
  python enrich.py
"""

import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
from google import genai
from google.genai import types
from google.cloud import bigquery
from pydantic import BaseModel, Field


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
REGION = os.environ.get("REGION", "us-central1")

# Modelo y parámetros.
# gemini-2.5-flash: versión estable de Flash en Vertex AI.
MODEL = "gemini-2.5-flash"
MAX_RETRIES = 3
BASE_DELAY_SECONDS = 4  # Backoff exponencial: 4s → 8s → 16s.
RATE_LIMIT_DELAY = 4  # Pausa entre productos para respetar rate limits.
BQ_BATCH_SIZE = 10  # Insertar en BQ cada N resultados exitosos.

# Categorías válidas para Resparked.
VALID_CATEGORIES = ["Herramientas", "Insumos", "Accesorios", "Materiales", "Kits"]

# System instruction — define el rol y contexto del modelo.
SYSTEM_INSTRUCTION = """
Eres el Especialista en Catalogación de Datos de la marca "Resparked". 
Tu objetivo es analizar nombres y descripciones de productos de e-commerce para devolver un enriquecimiento técnico preciso en formato JSON.

CONTEXTO DE MARCA:
Resparked es una marca líder en el ecosistema DIY de grabado artístico y marroquinería. 
Sus productos se dividen estrictamente en estas categorías:
1. Kits: Sets completos que incluyen herramienta y accesorios.
2. Herramientas: Dispositivos eléctricos de grabado (ej. Customizer Pen).
3. Insumos: Puntas (bits) de diamante o tungsteno, repuestos y fresas.
4. Accesorios: Estuches, cables, guías de práctica o stencils.
5. Materiales: Superficies para grabar, principalmente cuero vacuno y lienzos.

REGLAS DE SALIDA:
- Genera entre 3 y 6 tags técnicos (ej. "inalámbrico", "tungsteno", "cuero-vacuno").
- El campo 'confidence_score' debe ser un float entre 0.0 y 1.0.
- No incluyas explicaciones, solo el objeto JSON puro.
- Asegúrate de que 'tags' sea un array de strings nativo.
"""


# =============================================================================
# SCHEMA DE RESPUESTA (Pydantic)
# =============================================================================


class EnrichmentResult(BaseModel):
    """Schema estricto para la respuesta de Gemini.

    Pydantic valida tipos y constraints ANTES de que el resultado
    llegue a BigQuery — si Gemini devuelve algo fuera de spec, falla
    rápido en lugar de contaminar la tabla.
    """

    category: str = Field(description="Categoría principal del producto")
    subcategory: str = Field(description="Subcategoría específica")
    tags: list[str] = Field(
        min_length=3,
        max_length=6,
        description="Tags para búsqueda (entre 3 y 6)",
    )
    confidence_score: float = Field(
        ge=0.0,
        le=1.0,
        description="Score de confianza de la clasificación (0.0 a 1.0)",
    )


# =============================================================================
# ENRIQUECEDOR
# =============================================================================


class ProductEnricher:
    """Orquesta el enriquecimiento de productos con Gemini AI.

    Flujo sincrónico:
      1. get_pending_products() — Lee productos no enriquecidos de BQ.
      2. enrich_product()       — Envía cada producto a Gemini (sync).
      3. _flush_to_bigquery()   — Escribe batches de BQ_BATCH_SIZE.
      4. run()                  — Orquesta todo el flujo.
    """

    def __init__(self) -> None:
        if not GCP_PROJECT:
            raise ValueError(
                "GCP_PROJECT no está configurado. "
                "Exportalo con: export GCP_PROJECT=tu-proyecto"
            )

        # Vertex AI usa ADC (Application Default Credentials) en vez de API key.
        # En Cloud Shell las credenciales ya están disponibles automáticamente.
        self.genai_client = genai.Client(
            vertexai=True,
            project=GCP_PROJECT,
            location=REGION,
        )
        self.bq_client = bigquery.Client(project=GCP_PROJECT)

    def get_pending_products(self) -> pd.DataFrame:
        """Trae productos de final_products que NO están en enriched_products.

        Usa LEFT JOIN para hacer el proceso INCREMENTAL: si un producto
        ya fue enriquecido, no lo vuelve a procesar. Esto permite correr
        el script múltiples veces sin duplicar trabajo ni datos.

        Returns:
            DataFrame con los productos pendientes de enriquecer.
        """
        query = f"""
        SELECT
            f.product_id,
            f.name,
            f.description,
            f.price,
            f.supplier
        FROM `{GCP_PROJECT}.{DATASET_NAME}.final_products` AS f
        LEFT JOIN `{GCP_PROJECT}.{DATASET_NAME}.enriched_products` AS e
            ON f.product_id = e.product_id
        WHERE e.product_id IS NULL
        """

        logger.info("Consultando productos pendientes de enriquecer...")
        df = self.bq_client.query(query).to_dataframe()
        logger.info("Productos pendientes: %d", len(df))
        return df

    def enrich_product(self, row: dict) -> dict | None:
        """Enriquece UN producto con Gemini (sincrónico).

        Implementa retry con backoff exponencial: 4s → 8s → 16s.
        Si los 3 intentos fallan, loguea el product_id y retorna None
        sin interrumpir el batch.

        Args:
            row: Dict con product_id, name, description, price, supplier.

        Returns:
            Dict listo para BigQuery, o None si falló.
        """
        product_id = row["product_id"]
        prompt = (
            f"Clasificá el siguiente producto de Resparked:\n\n"
            f"Nombre: {row['name']}\n"
            f"Descripción: {row['description']}\n"
            f"Precio: ${row['price']}\n"
            f"Proveedor: {row['supplier']}"
        )

        for attempt in range(MAX_RETRIES):
            try:
                response = self.genai_client.models.generate_content(
                    model=MODEL,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        system_instruction=SYSTEM_INSTRUCTION,
                        response_mime_type="application/json",
                        response_schema=EnrichmentResult,
                        http_options=types.HttpOptions(timeout=25000),
                    ),
                )

                result: EnrichmentResult = response.parsed

                logger.info(
                    "✅ %s → %s / %s (%.2f)",
                    product_id,
                    result.category,
                    result.subcategory,
                    result.confidence_score,
                )

                return {
                    "product_id": product_id,
                    "category": result.category,
                    "subcategory": result.subcategory,
                    "tags": result.tags,
                    "confidence_score": result.confidence_score,
                    "enriched_at": datetime.now(timezone.utc).isoformat(),
                }

            except Exception as e:
                delay = BASE_DELAY_SECONDS * (2**attempt)
                logger.warning(
                    "⚠️  %s — intento %d/%d falló: %s. Reintentando en %ds...",
                    product_id,
                    attempt + 1,
                    MAX_RETRIES,
                    str(e),
                    delay,
                )
                if attempt < MAX_RETRIES - 1:
                    time.sleep(delay)

        # Los 3 intentos fallaron.
        logger.error(
            "❌ %s — falló después de %d intentos. Saltando.",
            product_id,
            MAX_RETRIES,
        )
        return None

    def _flush_to_bigquery(self, batch: list[dict]) -> None:
        """Inserta un batch de resultados en BigQuery.

        Método interno llamado cada vez que el buffer alcanza BQ_BATCH_SIZE
        o al final del procesamiento con el remanente.

        Args:
            batch: Lista de dicts a insertar (máximo BQ_BATCH_SIZE).

        Raises:
            RuntimeError: Si BigQuery reporta errores en el insert.
        """
        if not batch:
            return

        table_id = f"{GCP_PROJECT}.{DATASET_NAME}.enriched_products"

        logger.info("Insertando batch de %d filas en %s...", len(batch), table_id)

        errors = self.bq_client.insert_rows_json(table_id, batch)

        if errors:
            logger.error("Errores en BigQuery insert: %s", errors)
            raise RuntimeError(f"BigQuery streaming insert falló: {errors}")

        logger.info(
            "✅ Batch de %d filas insertado exitosamente",
            len(batch),
        )

    def run(self) -> None:
        """Orquesta el flujo completo de enriquecimiento.

        Loop sincrónico simple:
        1. Consulta productos pendientes en BigQuery.
        2. Enriquece cada uno con Gemini (sync, secuencial).
        3. Guarda en BQ en batches de BQ_BATCH_SIZE.
        4. Imprime resumen final.
        """
        start_time = time.time()

        # 1. Obtener productos pendientes.
        df = self.get_pending_products()

        if df.empty:
            logger.info("🎉 No hay productos pendientes. Todos ya fueron enriquecidos.")
            return

        # 2. Procesar secuencialmente con rate limiting.
        buffer: list[dict] = []
        succeeded = 0
        failed = 0

        for idx, row in df.iterrows():
            result = self.enrich_product(row.to_dict())

            if result is not None:
                buffer.append(result)
                succeeded += 1

                # Flush batch cuando el buffer se llena.
                if len(buffer) >= BQ_BATCH_SIZE:
                    self._flush_to_bigquery(buffer)
                    buffer.clear()
            else:
                failed += 1

            # Rate limit: pausa entre productos.
            time.sleep(RATE_LIMIT_DELAY)

        # Flush del remanente (< BQ_BATCH_SIZE).
        if buffer:
            self._flush_to_bigquery(buffer)

        # 3. Resumen final.
        elapsed = time.time() - start_time
        logger.info("━" * 50)
        logger.info("RESUMEN DE ENRIQUECIMIENTO")
        logger.info("━" * 50)
        logger.info("Total procesados:  %d", len(df))
        logger.info("Exitosos:          %d", succeeded)
        logger.info("Fallidos:          %d", failed)
        logger.info("Tiempo total:      %.1fs", elapsed)
        logger.info("━" * 50)


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    ProductEnricher().run()
