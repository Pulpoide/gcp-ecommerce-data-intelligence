"""
Script de Enriquecimiento de Productos con Gemini AI
=====================================================

Módulo 02 del pipeline de Resparked Data Intelligence.

Lee productos de final_products que aún no fueron enriquecidos,
los envía a Gemini 1.5 Flash para clasificación automática,
y escribe los resultados en enriched_products.

El procesamiento es INCREMENTAL: solo enriquece productos que no
tienen entrada en enriched_products (LEFT JOIN + IS NULL).

Variables de entorno requeridas:
  - GEMINI_API_KEY: API key de Google AI Studio para Gemini.
  - GCP_PROJECT:    ID del proyecto de GCP.
  - DATASET_NAME:   Nombre del dataset de BigQuery (default: dropshipping).

Uso:
  export GEMINI_API_KEY=tu-api-key
  export GCP_PROJECT=tu-proyecto
  python enrich.py
"""

import asyncio
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

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GCP_PROJECT = os.environ.get("GCP_PROJECT", "")
DATASET_NAME = os.environ.get("DATASET_NAME", "dropshipping")

# Modelo y parámetros de concurrencia.
MODEL = "gemini-2.0-flash"
MAX_CONCURRENT = 5
MAX_RETRIES = 3
BASE_DELAY_SECONDS = 2  # Backoff exponencial: 2s → 4s → 8s.

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

    Flujo:
      1. get_pending_products() — Lee productos no enriquecidos de BQ.
      2. enrich_product()       — Envía cada producto a Gemini (async).
      3. save_to_bigquery()     — Escribe todos los resultados en un solo batch.
      4. run()                  — Orquesta todo el flujo.
    """

    def __init__(self) -> None:
        if not GEMINI_API_KEY:
            raise ValueError(
                "GEMINI_API_KEY no está configurada. "
                "Exportala con: export GEMINI_API_KEY=tu-api-key"
            )
        if not GCP_PROJECT:
            raise ValueError(
                "GCP_PROJECT no está configurado. "
                "Exportalo con: export GCP_PROJECT=tu-proyecto"
            )

        self.genai_client = genai.Client(api_key=GEMINI_API_KEY)
        self.bq_client = bigquery.Client(project=GCP_PROJECT)
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)

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

    async def enrich_product(self, row: dict) -> dict | None:
        """Enriquece UN producto con Gemini 1.5 Flash.

        Usa Semaphore para limitar concurrencia a MAX_CONCURRENT requests.
        Implementa retry con backoff exponencial: 2s → 4s → 8s.

        Si los 3 intentos fallan, loguea el product_id y retorna None
        sin interrumpir el batch completo.

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

        async with self.semaphore:
            for attempt in range(MAX_RETRIES):
                try:
                    response = await self.genai_client.aio.models.generate_content(
                        model=MODEL,
                        contents=prompt,
                        config=types.GenerateContentConfig(
                            system_instruction=SYSTEM_INSTRUCTION,
                            response_mime_type="application/json",
                            response_schema=EnrichmentResult,
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
                        await asyncio.sleep(delay)

        # Los 3 intentos fallaron.
        logger.error(
            "❌ %s — falló después de %d intentos. Saltando.",
            product_id,
            MAX_RETRIES,
        )
        return None

    def save_to_bigquery(self, results: list[dict]) -> None:
        """Escribe TODOS los resultados en BigQuery en un solo batch.

        Usa insert_rows_json() (streaming insert). Es más simple que
        load_table_from_dataframe() y no requiere pyarrow para la escritura.

        El campo `tags` (Python list) se mapea automáticamente a
        ARRAY<STRING> en BigQuery.

        Args:
            results: Lista de dicts con el resultado del enriquecimiento.

        Raises:
            RuntimeError: Si BigQuery reporta errores en el insert.
        """
        table_id = f"{GCP_PROJECT}.{DATASET_NAME}.enriched_products"

        logger.info("Insertando %d filas en %s...", len(results), table_id)

        errors = self.bq_client.insert_rows_json(table_id, results)

        if errors:
            logger.error("Errores en BigQuery insert: %s", errors)
            raise RuntimeError(f"BigQuery streaming insert falló: {errors}")

        logger.info(
            "✅ %d filas insertadas exitosamente en %s",
            len(results),
            table_id,
        )

    async def _enrich_all(self, df: pd.DataFrame) -> list[dict | None]:
        """Lanza enriquecimiento de TODOS los productos en paralelo.

        asyncio.gather() ejecuta todas las coroutines concurrentemente,
        pero el Semaphore interno limita a MAX_CONCURRENT simultáneos.
        """
        tasks = [self.enrich_product(row.to_dict()) for _, row in df.iterrows()]
        return await asyncio.gather(*tasks)

    def run(self) -> None:
        """Orquesta el flujo completo de enriquecimiento.

        1. Consulta productos pendientes en BigQuery.
        2. Enriquece cada uno con Gemini (async, concurrente).
        3. Escribe los resultados en enriched_products.
        4. Imprime resumen final.
        """
        start_time = time.time()

        # 1. Obtener productos pendientes.
        df = self.get_pending_products()

        if df.empty:
            logger.info("🎉 No hay productos pendientes. Todos ya fueron enriquecidos.")
            return

        # 2. Enriquecer con Gemini (async).
        all_results = asyncio.run(self._enrich_all(df))

        # 3. Separar exitosos de fallidos.
        successful = [r for r in all_results if r is not None]
        failed_count = len(all_results) - len(successful)

        # 4. Guardar en BigQuery (un solo batch).
        if successful:
            self.save_to_bigquery(successful)
        else:
            logger.warning("⚠️  Ningún producto fue enriquecido exitosamente.")

        # 5. Resumen final.
        elapsed = time.time() - start_time
        logger.info("━" * 50)
        logger.info("RESUMEN DE ENRIQUECIMIENTO")
        logger.info("━" * 50)
        logger.info("Total procesados:  %d", len(all_results))
        logger.info("Exitosos:          %d", len(successful))
        logger.info("Fallidos:          %d", failed_count)
        logger.info("Tiempo total:      %.1fs", elapsed)
        logger.info("━" * 50)


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    ProductEnricher().run()
