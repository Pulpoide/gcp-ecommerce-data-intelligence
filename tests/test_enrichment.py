"""
Tests para el módulo de enriquecimiento AI (02_ai_enrichment).

Estos tests validan la lógica de negocio SIN depender de servicios
externos (BigQuery, Gemini). Todos los clientes se mockean.

Qué se testea:
  - Validación del schema Pydantic (EnrichmentResult).
  - Lógica de get_pending_products (query SQL correcta).
  - Lógica de enrich_product (happy path, retry, fallo total).
  - Lógica de save_to_bigquery (batch insert, manejo de errores).
  - Orquestación completa del run().

NOTA sobre mocks de env vars:
  Las variables GCP_PROJECT, etc. se leen como constantes a nivel de módulo
  en enrich.py. Para que los tests las overriteen correctamente, se patchean
  las constantes del módulo directamente (patch("enrich.GCP_PROJECT", ...))
  en vez de os.environ.
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from pydantic import ValidationError

# Importamos desde el módulo bajo test.
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "02_ai_enrichment"))

from enrich import EnrichmentResult, ProductEnricher


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def sample_row():
    """Fila de ejemplo simulando un producto de final_products."""
    return {
        "product_id": "RSP-001",
        "name": "Customizer Pen Pro",
        "description": "Bolígrafo de grabado inalámbrico con punta de tungsteno",
        "price": 89.99,
        "supplier": "Resparked",
    }


@pytest.fixture
def sample_enrichment():
    """Resultado de enriquecimiento válido (simula respuesta de Gemini)."""
    return EnrichmentResult(
        category="Herramientas",
        subcategory="Bolígrafos de Grabado",
        tags=["inalámbrico", "tungsteno", "grabado", "profesional"],
        confidence_score=0.95,
    )


@pytest.fixture
def sample_dataframe():
    """DataFrame con 3 productos de ejemplo."""
    return pd.DataFrame(
        [
            {
                "product_id": "RSP-001",
                "name": "Customizer Pen Pro",
                "description": "Bolígrafo inalámbrico",
                "price": 89.99,
                "supplier": "Resparked",
            },
            {
                "product_id": "RSP-002",
                "name": "Diamond Bit Set",
                "description": "Set de 5 puntas de diamante",
                "price": 24.99,
                "supplier": "GrabaTech",
            },
            {
                "product_id": "RSP-003",
                "name": "Premium Leather Sheet",
                "description": "Cuero vacuno premium para grabado",
                "price": 15.00,
                "supplier": "Resparked",
            },
        ]
    )


# Helper: patches comunes para construir ProductEnricher sin env vars reales.
def _env_patches():
    """Retorna los patches necesarios para instanciar ProductEnricher en tests."""
    return [
        patch("enrich.GCP_PROJECT", "test-project"),
        patch("enrich.genai.Client"),
        patch("enrich.bigquery.Client"),
    ]


# =============================================================================
# TESTS: EnrichmentResult (Schema Pydantic)
# =============================================================================


class TestEnrichmentResult:
    """Valida que el schema Pydantic rechace datos fuera de spec."""

    def test_valid_result(self):
        """Resultado válido con todos los campos correctos."""
        result = EnrichmentResult(
            category="Herramientas",
            subcategory="Bolígrafos de Grabado",
            tags=["tungsteno", "inalámbrico", "grabado"],
            confidence_score=0.92,
        )
        assert result.category == "Herramientas"
        assert len(result.tags) == 3
        assert 0.0 <= result.confidence_score <= 1.0

    def test_tags_minimum_3(self):
        """Debe rechazar si tags tiene menos de 3 elementos."""
        with pytest.raises(ValidationError):
            EnrichmentResult(
                category="Herramientas",
                subcategory="Bolígrafos",
                tags=["solo", "dos"],
                confidence_score=0.9,
            )

    def test_tags_maximum_6(self):
        """Debe rechazar si tags tiene más de 6 elementos."""
        with pytest.raises(ValidationError):
            EnrichmentResult(
                category="Herramientas",
                subcategory="Bolígrafos",
                tags=["a", "b", "c", "d", "e", "f", "g"],
                confidence_score=0.9,
            )

    def test_confidence_score_below_zero(self):
        """Debe rechazar score negativo."""
        with pytest.raises(ValidationError):
            EnrichmentResult(
                category="Herramientas",
                subcategory="Bolígrafos",
                tags=["a", "b", "c"],
                confidence_score=-0.1,
            )

    def test_confidence_score_above_one(self):
        """Debe rechazar score mayor a 1.0."""
        with pytest.raises(ValidationError):
            EnrichmentResult(
                category="Herramientas",
                subcategory="Bolígrafos",
                tags=["a", "b", "c"],
                confidence_score=1.5,
            )

    def test_tags_exactly_3_and_6(self):
        """Límites exactos: 3 tags y 6 tags deben ser válidos."""
        result_min = EnrichmentResult(
            category="Kits",
            subcategory="Kit Starter",
            tags=["kit", "starter", "grabado"],
            confidence_score=0.8,
        )
        result_max = EnrichmentResult(
            category="Kits",
            subcategory="Kit Starter",
            tags=["kit", "starter", "grabado", "cuero", "DIY", "profesional"],
            confidence_score=0.8,
        )
        assert len(result_min.tags) == 3
        assert len(result_max.tags) == 6


# =============================================================================
# TESTS: ProductEnricher.get_pending_products()
# =============================================================================


class TestGetPendingProducts:
    """Valida que la query SQL sea correcta y la lógica incremental funcione."""

    def test_returns_dataframe_from_query(self, sample_dataframe):
        """Debe ejecutar la query y retornar un DataFrame."""
        with (
            patch("enrich.GCP_PROJECT", "test-project"),
            patch("enrich.genai.Client"),
            patch("enrich.bigquery.Client") as mock_bq_class,
        ):
            mock_bq = MagicMock()
            mock_bq.query.return_value.to_dataframe.return_value = sample_dataframe
            mock_bq_class.return_value = mock_bq

            enricher = ProductEnricher()
            result = enricher.get_pending_products()

            # Verifica que se llamó a bq.query con la SQL correcta.
            call_args = mock_bq.query.call_args[0][0]
            assert "LEFT JOIN" in call_args
            assert "WHERE e.product_id IS NULL" in call_args
            assert "final_products" in call_args
            assert "enriched_products" in call_args
            assert len(result) == 3

    def test_returns_empty_when_all_enriched(self):
        """Si todos los productos ya fueron enriquecidos, retorna DF vacío."""
        with (
            patch("enrich.GCP_PROJECT", "test-project"),
            patch("enrich.genai.Client"),
            patch("enrich.bigquery.Client") as mock_bq_class,
        ):
            mock_bq = MagicMock()
            mock_bq.query.return_value.to_dataframe.return_value = pd.DataFrame()
            mock_bq_class.return_value = mock_bq

            enricher = ProductEnricher()
            result = enricher.get_pending_products()
            assert result.empty


# =============================================================================
# TESTS: ProductEnricher.enrich_product()
# =============================================================================


class TestEnrichProduct:
    """Valida la lógica de enriquecimiento: happy path, retry y fallo."""

    def test_happy_path(self, sample_row, sample_enrichment):
        """Gemini responde bien → retorna dict con todos los campos."""
        with (
            patch("enrich.GCP_PROJECT", "test-project"),
            patch("enrich.genai.Client") as mock_genai_class,
            patch("enrich.bigquery.Client"),
        ):
            mock_genai = MagicMock()
            mock_response = MagicMock()
            mock_response.parsed = sample_enrichment
            mock_genai.aio.models.generate_content = AsyncMock(
                return_value=mock_response
            )
            mock_genai_class.return_value = mock_genai

            enricher = ProductEnricher()
            result = asyncio.run(enricher.enrich_product(sample_row))

            assert result is not None
            assert result["product_id"] == "RSP-001"
            assert result["category"] == "Herramientas"
            assert result["subcategory"] == "Bolígrafos de Grabado"
            assert len(result["tags"]) == 4
            assert result["confidence_score"] == 0.95
            assert "enriched_at" in result

    def test_retry_then_success(self, sample_row, sample_enrichment):
        """Falla 2 veces, funciona la 3ra → retorna resultado válido."""
        with (
            patch("enrich.GCP_PROJECT", "test-project"),
            patch("enrich.genai.Client") as mock_genai_class,
            patch("enrich.bigquery.Client"),
            patch("enrich.asyncio.sleep", new_callable=AsyncMock),
        ):
            mock_genai = MagicMock()
            mock_response = MagicMock()
            mock_response.parsed = sample_enrichment

            mock_genai.aio.models.generate_content = AsyncMock(
                side_effect=[
                    Exception("Rate limit"),
                    Exception("Timeout"),
                    mock_response,
                ]
            )
            mock_genai_class.return_value = mock_genai

            enricher = ProductEnricher()
            result = asyncio.run(enricher.enrich_product(sample_row))

            assert result is not None
            assert result["product_id"] == "RSP-001"

    def test_all_retries_fail_returns_none(self, sample_row):
        """Los 3 intentos fallan → retorna None sin crashear."""
        with (
            patch("enrich.GCP_PROJECT", "test-project"),
            patch("enrich.genai.Client") as mock_genai_class,
            patch("enrich.bigquery.Client"),
            patch("enrich.asyncio.sleep", new_callable=AsyncMock),
        ):
            mock_genai = MagicMock()
            mock_genai.aio.models.generate_content = AsyncMock(
                side_effect=Exception("Persistent error")
            )
            mock_genai_class.return_value = mock_genai

            enricher = ProductEnricher()
            result = asyncio.run(enricher.enrich_product(sample_row))
            assert result is None


# =============================================================================
# TESTS: ProductEnricher._flush_to_bigquery()
# =============================================================================


class TestFlushToBigQuery:
    """Valida la escritura de batches a BigQuery."""

    def test_successful_insert(self):
        """insert_rows_json sin errores → no lanza excepción."""
        with (
            patch("enrich.GCP_PROJECT", "test-project"),
            patch("enrich.genai.Client"),
            patch("enrich.bigquery.Client") as mock_bq_class,
        ):
            mock_bq = MagicMock()
            mock_bq.insert_rows_json.return_value = []
            mock_bq_class.return_value = mock_bq

            enricher = ProductEnricher()
            batch = [
                {
                    "product_id": "RSP-001",
                    "category": "Herramientas",
                    "subcategory": "Bolígrafos",
                    "tags": ["a", "b", "c"],
                    "confidence_score": 0.9,
                    "enriched_at": "2026-04-23T21:00:00+00:00",
                }
            ]
            enricher._flush_to_bigquery(batch)
            mock_bq.insert_rows_json.assert_called_once()

    def test_insert_errors_raise_runtime(self):
        """BigQuery reporta errores → lanza RuntimeError."""
        with (
            patch("enrich.GCP_PROJECT", "test-project"),
            patch("enrich.genai.Client"),
            patch("enrich.bigquery.Client") as mock_bq_class,
        ):
            mock_bq = MagicMock()
            mock_bq.insert_rows_json.return_value = [
                {"index": 0, "errors": [{"reason": "invalid"}]}
            ]
            mock_bq_class.return_value = mock_bq

            enricher = ProductEnricher()
            with pytest.raises(RuntimeError, match="BigQuery streaming insert falló"):
                enricher._flush_to_bigquery([{"product_id": "RSP-001"}])

    def test_empty_batch_skipped(self):
        """Batch vacío → no llama a insert_rows_json."""
        with (
            patch("enrich.GCP_PROJECT", "test-project"),
            patch("enrich.genai.Client"),
            patch("enrich.bigquery.Client") as mock_bq_class,
        ):
            mock_bq = MagicMock()
            mock_bq_class.return_value = mock_bq

            enricher = ProductEnricher()
            enricher._flush_to_bigquery([])
            mock_bq.insert_rows_json.assert_not_called()


# =============================================================================
# TESTS: ProductEnricher.run() — orquestación
# =============================================================================


class TestRun:
    """Valida el flujo completo de orquestación."""

    def test_skips_when_no_pending(self):
        """Si no hay productos pendientes, no llama a Gemini ni a BQ insert."""
        with (
            patch("enrich.GCP_PROJECT", "test-project"),
            patch("enrich.genai.Client"),
            patch("enrich.bigquery.Client") as mock_bq_class,
        ):
            mock_bq = MagicMock()
            mock_bq.query.return_value.to_dataframe.return_value = pd.DataFrame()
            mock_bq_class.return_value = mock_bq

            enricher = ProductEnricher()
            enricher.run()
            mock_bq.insert_rows_json.assert_not_called()

    def test_full_flow_with_mixed_results(self, sample_dataframe):
        """3 productos: 2 exitosos, 1 falla → solo 2 se guardan en BQ."""
        with (
            patch("enrich.GCP_PROJECT", "test-project"),
            patch("enrich.genai.Client") as mock_genai_class,
            patch("enrich.bigquery.Client") as mock_bq_class,
            patch("enrich.asyncio.sleep", new_callable=AsyncMock),
        ):
            mock_bq = MagicMock()
            mock_bq.query.return_value.to_dataframe.return_value = sample_dataframe
            mock_bq.insert_rows_json.return_value = []
            mock_bq_class.return_value = mock_bq

            success_response = MagicMock()
            success_response.parsed = EnrichmentResult(
                category="Herramientas",
                subcategory="Bolígrafos",
                tags=["tag1", "tag2", "tag3"],
                confidence_score=0.9,
            )

            mock_genai = MagicMock()
            # 2 éxitos + 3 fallos (3 retries para el tercer producto).
            mock_genai.aio.models.generate_content = AsyncMock(
                side_effect=[
                    success_response,
                    success_response,
                    Exception("API Error"),
                    Exception("API Error"),
                    Exception("API Error"),
                ]
            )
            mock_genai_class.return_value = mock_genai

            enricher = ProductEnricher()
            enricher.run()

            call_args = mock_bq.insert_rows_json.call_args
            inserted_rows = call_args[0][1]
            assert len(inserted_rows) == 2


# =============================================================================
# TESTS: Validación de env vars
# =============================================================================


class TestEnvValidation:
    """Verifica que el constructor falle sin variables de entorno."""

    def test_missing_project_raises(self):
        """Sin GCP_PROJECT → ValueError."""
        with (
            patch("enrich.GCP_PROJECT", ""),
            patch("enrich.genai.Client"),
        ):
            with pytest.raises(ValueError, match="GCP_PROJECT"):
                ProductEnricher()
