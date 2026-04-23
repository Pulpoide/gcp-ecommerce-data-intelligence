-- =============================================================================
-- staging → final_products
-- =============================================================================
--
-- Promueve los datos validados de staging_products a final_products.
-- Usa MERGE para garantizar IDEMPOTENCIA: podés correr este script N veces
-- y siempre vas a obtener el mismo resultado correcto.
--
-- Lógica:
--   - Si el product_id YA existe en final y staging tiene una versión más
--     nueva (ingested_at mayor) → UPDATE.
--   - Si el product_id NO existe en final → INSERT.
--   - Si el product_id existe pero staging no tiene nada más nuevo → no hace nada.
--
-- Uso desde Cloud Shell:
--   bq query --use_legacy_sql=false < infra/staging_to_final.sql
--
-- =============================================================================

MERGE `dropshipping.final_products` AS target
USING (
  -- Deduplicamos staging: nos quedamos con la versión más reciente de cada SKU.
  -- Esto cubre el caso de que el mismo product_id haya sido ingestado varias veces.
  SELECT product_id, name, description, price, supplier, stock, ingested_at
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY product_id
        ORDER BY ingested_at DESC
      ) AS rn
    FROM `dropshipping.staging_products`
  )
  WHERE rn = 1
) AS source
ON target.product_id = source.product_id

-- Caso 1: el producto ya existe en final pero staging tiene una versión más nueva.
WHEN MATCHED AND source.ingested_at > target.ingested_at THEN
  UPDATE SET
    name        = source.name,
    description = source.description,
    price       = source.price,
    supplier    = source.supplier,
    stock       = source.stock,
    ingested_at = source.ingested_at

-- Caso 2: el producto no existe en final → insertar.
WHEN NOT MATCHED THEN
  INSERT (product_id, name, description, price, supplier, stock, ingested_at)
  VALUES (
    source.product_id,
    source.name,
    source.description,
    source.price,
    source.supplier,
    source.stock,
    source.ingested_at
  );
