"""
Tests para el módulo de monitoreo de precios (03_price_monitoring).

Estos tests validan la lógica de negocio SIN depender de servicios
externos (BigQuery, Pub/Sub). Todos los clientes se mockean.

Qué se testea:

  Simulador (simulator.py):
    - Carga inicial de productos desde BigQuery.
    - Generación de precios con factor aleatorio.
    - Cálculo correcto de change_percent.
    - Clasificación de alertas (>10% = WARNING, <=10% = INFO).
    - Publicación de mensajes a Pub/Sub con formato correcto.

  Cloud Function (main.py):
    - Decodificación de mensaje Pub/Sub base64.
    - Validación de campos requeridos en el JSON.
    - Inserción en BigQuery (happy path).
    - Manejo de errores: JSON inválido → discard, BQ error → re-raise.
"""

import base64
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "03_price_monitoring"))


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def sample_products_df():
    """DataFrame simulando final_products con product_id y price."""
    return pd.DataFrame(
        [
            {"product_id": "RSP-001", "price": 100.00},
            {"product_id": "RSP-002", "price": 50.00},
            {"product_id": "RSP-003", "price": 200.00},
        ]
    )


@pytest.fixture
def sample_price_message():
    """Mensaje de precio válido como llegaría a la Cloud Function."""
    return {
        "product_id": "RSP-001",
        "old_price": 100.00,
        "new_price": 115.00,
        "change_percent": 15.0,
        "timestamp": "2026-04-24T12:00:00+00:00",
    }


@pytest.fixture
def make_cloud_event(sample_price_message):
    """Factory para crear CloudEvents de Pub/Sub con payload configurable."""

    def _make(payload: dict | None = None):
        data = payload if payload is not None else sample_price_message
        encoded = base64.b64encode(json.dumps(data).encode("utf-8")).decode("utf-8")
        return MagicMock(
            data={"message": {"data": encoded}},
        )

    return _make


# =============================================================================
# TESTS: Simulador — Carga de productos
# =============================================================================


class TestSimulatorLoadProducts:
    """Valida la carga inicial de productos desde BigQuery."""

    def test_loads_products_from_bigquery(self, sample_products_df):
        """Debe leer product_id y price de final_products."""
        with (
            patch("simulator.GCP_PROJECT", "test-project"),
            patch("simulator.bigquery.Client") as mock_bq_class,
            patch("simulator.pubsub_v1.PublisherClient"),
        ):
            mock_bq = MagicMock()
            mock_bq.query.return_value.to_dataframe.return_value = sample_products_df
            mock_bq_class.return_value = mock_bq

            from simulator import load_products

            result = load_products()

            assert len(result) == 3
            # Verifica que la query contiene las columnas correctas.
            call_args = mock_bq.query.call_args[0][0]
            assert "product_id" in call_args
            assert "price" in call_args
            assert "final_products" in call_args

    def test_empty_table_returns_empty_dict(self):
        """Si no hay productos, retorna dict vacío."""
        with (
            patch("simulator.GCP_PROJECT", "test-project"),
            patch("simulator.bigquery.Client") as mock_bq_class,
            patch("simulator.pubsub_v1.PublisherClient"),
        ):
            mock_bq = MagicMock()
            mock_bq.query.return_value.to_dataframe.return_value = pd.DataFrame(
                columns=["product_id", "price"]
            )
            mock_bq_class.return_value = mock_bq

            from simulator import load_products

            result = load_products()
            assert result == {}


# =============================================================================
# TESTS: Simulador — Generación de precios
# =============================================================================


class TestSimulatorPriceGeneration:
    """Valida la lógica de generación de precios aleatorios."""

    def test_price_within_factor_range(self):
        """Nuevo precio debe estar entre old_price * 0.8 y old_price * 1.2."""
        from simulator import generate_new_price

        old_price = 100.00
        # Ejecutamos varias veces para cubrir el rango aleatorio.
        for _ in range(100):
            new_price = generate_new_price(old_price)
            assert 80.00 <= new_price <= 120.00, (
                f"Precio {new_price} fuera del rango [80, 120]"
            )

    def test_change_percent_calculation(self):
        """change_percent debe calcularse correctamente."""
        from simulator import calculate_change_percent

        assert calculate_change_percent(100.0, 115.0) == 15.0
        assert calculate_change_percent(100.0, 85.0) == -15.0
        assert calculate_change_percent(100.0, 100.0) == 0.0
        assert calculate_change_percent(50.0, 60.0) == pytest.approx(20.0)


# =============================================================================
# TESTS: Simulador — Clasificación de alertas
# =============================================================================


class TestSimulatorAlerts:
    """Valida que cambios >10% se logueen como WARNING."""

    def test_large_change_is_warning(self):
        """change_percent > 10% → WARNING."""
        from simulator import is_alert

        assert is_alert(15.0) is True
        assert is_alert(-12.0) is True
        assert is_alert(10.1) is True

    def test_small_change_is_info(self):
        """change_percent <= 10% → INFO (no alerta)."""
        from simulator import is_alert

        assert is_alert(5.0) is False
        assert is_alert(-5.0) is False
        assert is_alert(10.0) is False
        assert is_alert(0.0) is False


# =============================================================================
# TESTS: Simulador — Publicación a Pub/Sub
# =============================================================================


class TestSimulatorPublish:
    """Valida el formato del mensaje publicado a Pub/Sub."""

    def test_publish_message_format(self):
        """El mensaje publicado debe tener todos los campos requeridos."""
        with (
            patch("simulator.GCP_PROJECT", "test-project"),
            patch("simulator.bigquery.Client"),
            patch("simulator.pubsub_v1.PublisherClient") as mock_pub_class,
        ):
            mock_publisher = MagicMock()
            mock_pub_class.return_value = mock_publisher
            mock_publisher.topic_path.return_value = (
                "projects/test-project/topics/price-updates"
            )

            from simulator import publish_price_update

            publish_price_update(
                product_id="RSP-001",
                old_price=100.0,
                new_price=115.0,
                change_percent=15.0,
            )

            # Verificar que se llamó a publish.
            mock_publisher.publish.assert_called_once()
            call_args = mock_publisher.publish.call_args

            # El primer argumento posicional es el topic.
            topic = call_args[0][0]
            assert topic == "projects/test-project/topics/price-updates"

            # El keyword argument 'data' es bytes JSON.
            data_bytes = call_args[1]["data"]
            message = json.loads(data_bytes.decode("utf-8"))

            assert message["product_id"] == "RSP-001"
            assert message["old_price"] == 100.0
            assert message["new_price"] == 115.0
            assert message["change_percent"] == 15.0
            assert "timestamp" in message


# =============================================================================
# TESTS: Cloud Function — Decodificación de mensajes
# =============================================================================


class TestCloudFunctionDecode:
    """Valida la decodificación del mensaje Pub/Sub."""

    def test_valid_message_decoded(self, make_cloud_event, sample_price_message):
        """Mensaje válido en base64 → dict con campos correctos."""
        from main import decode_pubsub_message

        cloud_event = make_cloud_event()
        result = decode_pubsub_message(cloud_event)

        assert result["product_id"] == "RSP-001"
        assert result["old_price"] == 100.00
        assert result["new_price"] == 115.00
        assert result["change_percent"] == 15.0
        assert "timestamp" in result

    def test_invalid_base64_returns_none(self):
        """Base64 inválido → retorna None."""
        from main import decode_pubsub_message

        event = MagicMock(data={"message": {"data": "!!!not-base64!!!"}})
        result = decode_pubsub_message(event)
        assert result is None

    def test_invalid_json_returns_none(self):
        """Base64 válido pero contenido no es JSON → retorna None."""
        from main import decode_pubsub_message

        raw = base64.b64encode(b"esto no es json").decode("utf-8")
        event = MagicMock(data={"message": {"data": raw}})
        result = decode_pubsub_message(event)
        assert result is None


# =============================================================================
# TESTS: Cloud Function — Validación de campos
# =============================================================================


class TestCloudFunctionValidation:
    """Valida que se verifiquen los campos requeridos del mensaje."""

    def test_valid_message_passes(self, sample_price_message):
        """Mensaje con todos los campos → True."""
        from main import validate_message

        assert validate_message(sample_price_message) is True

    def test_missing_field_fails(self, sample_price_message):
        """Mensaje sin un campo requerido → False."""
        from main import validate_message

        del sample_price_message["product_id"]
        assert validate_message(sample_price_message) is False

    def test_empty_message_fails(self):
        """Mensaje vacío → False."""
        from main import validate_message

        assert validate_message({}) is False

    def test_partial_message_fails(self):
        """Mensaje con solo algunos campos → False."""
        from main import validate_message

        partial = {"product_id": "RSP-001", "old_price": 100.0}
        assert validate_message(partial) is False


# =============================================================================
# TESTS: Cloud Function — Inserción en BigQuery
# =============================================================================


class TestCloudFunctionInsert:
    """Valida la inserción en la tabla price_history."""

    def test_successful_insert(self, sample_price_message):
        """insert_rows_json sin errores → no lanza excepción."""
        with (
            patch("main.GCP_PROJECT", "test-project"),
            patch("main.bigquery.Client") as mock_bq_class,
        ):
            mock_bq = MagicMock()
            mock_bq.insert_rows_json.return_value = []
            mock_bq_class.return_value = mock_bq

            from main import insert_to_bigquery

            insert_to_bigquery(sample_price_message)

            mock_bq.insert_rows_json.assert_called_once()
            call_args = mock_bq.insert_rows_json.call_args
            table_id = call_args[0][0]
            assert "price_history" in table_id

            rows = call_args[0][1]
            assert len(rows) == 1
            assert rows[0]["product_id"] == "RSP-001"
            assert "updated_at" in rows[0]

    def test_bq_errors_raise_exception(self, sample_price_message):
        """Errores de BigQuery → relanza excepción para retry."""
        with (
            patch("main.GCP_PROJECT", "test-project"),
            patch("main.bigquery.Client") as mock_bq_class,
        ):
            mock_bq = MagicMock()
            mock_bq.insert_rows_json.return_value = [
                {"index": 0, "errors": [{"reason": "invalid"}]}
            ]
            mock_bq_class.return_value = mock_bq

            from main import insert_to_bigquery

            with pytest.raises(RuntimeError, match="BigQuery"):
                insert_to_bigquery(sample_price_message)


# =============================================================================
# TESTS: Cloud Function — Flujo completo (process_price_update)
# =============================================================================


class TestCloudFunctionFlow:
    """Valida el flujo completo del entry point."""

    def test_valid_message_inserts_to_bq(self, make_cloud_event):
        """Mensaje válido → se inserta en BigQuery."""
        with (
            patch("main.GCP_PROJECT", "test-project"),
            patch("main.bigquery.Client") as mock_bq_class,
        ):
            mock_bq = MagicMock()
            mock_bq.insert_rows_json.return_value = []
            mock_bq_class.return_value = mock_bq

            from main import process_price_update

            cloud_event = make_cloud_event()
            process_price_update(cloud_event)

            mock_bq.insert_rows_json.assert_called_once()

    def test_invalid_json_does_not_insert(self):
        """JSON inválido → no llama a insert, no relanza."""
        with (
            patch("main.GCP_PROJECT", "test-project"),
            patch("main.bigquery.Client") as mock_bq_class,
        ):
            mock_bq = MagicMock()
            mock_bq_class.return_value = mock_bq

            from main import process_price_update

            raw = base64.b64encode(b"not json").decode("utf-8")
            event = MagicMock(data={"message": {"data": raw}})

            # No debe lanzar — discard silencioso.
            process_price_update(event)
            mock_bq.insert_rows_json.assert_not_called()

    def test_missing_fields_does_not_insert(self, make_cloud_event):
        """Campos faltantes → no inserta, no relanza."""
        with (
            patch("main.GCP_PROJECT", "test-project"),
            patch("main.bigquery.Client") as mock_bq_class,
        ):
            mock_bq = MagicMock()
            mock_bq_class.return_value = mock_bq

            from main import process_price_update

            cloud_event = make_cloud_event({"product_id": "RSP-001"})  # Incompleto.
            process_price_update(cloud_event)
            mock_bq.insert_rows_json.assert_not_called()

    def test_bq_error_raises_for_retry(self, make_cloud_event):
        """Error de BQ → relanza para que Cloud Functions reintente."""
        with (
            patch("main.GCP_PROJECT", "test-project"),
            patch("main.bigquery.Client") as mock_bq_class,
        ):
            mock_bq = MagicMock()
            mock_bq.insert_rows_json.return_value = [
                {"index": 0, "errors": [{"reason": "invalid"}]}
            ]
            mock_bq_class.return_value = mock_bq

            from main import process_price_update

            cloud_event = make_cloud_event()
            with pytest.raises(RuntimeError):
                process_price_update(cloud_event)
