-- =============================================================================
-- Vista: dropshipping.v_dashboard_products
-- Proyecto: project-b8b7f8b6-4ca7-4b6e-96b
-- =============================================================================
--
-- Consolida en una sola vista los datos del pipeline completo:
--   - Productos base (final_products)
--   - Enriquecimiento AI (enriched_products)
--   - Último precio registrado (price_history)
--
-- Uso: conectar directamente a Looker Studio como fuente de datos.
-- =============================================================================

CREATE OR REPLACE VIEW `project-b8b7f8b6-4ca7-4b6e-96b.dropshipping.v_dashboard_products` AS

-- -------------------------------------------------------------------------
-- CTE: latest_prices
-- Obtiene el último registro de price_history por producto.
-- ROW_NUMBER() garantiza exactamente 1 fila por product_id (la más reciente).
-- -------------------------------------------------------------------------
WITH latest_prices AS (
  SELECT
    product_id,
    old_price,
    new_price,
    change_percent        AS last_change_percent,
    updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY updated_at DESC
    )                     AS rn
  FROM
    `project-b8b7f8b6-4ca7-4b6e-96b.dropshipping.price_history`
)

-- -------------------------------------------------------------------------
-- SELECT principal
-- -------------------------------------------------------------------------
SELECT
  -- Datos base del producto
  fp.product_id,
  fp.name,
  fp.stock,

  -- Categorización AI (NULL si el producto aún no fue enriquecido)
  ep.category,
  ep.subcategory,
  ARRAY_TO_STRING(ep.tags, ', ')        AS tags_string,

  -- Precio actual: usa el último precio del simulador si existe,
  -- o el precio original de final_products como fallback.
  COALESCE(lp.new_price, fp.price)      AS current_price,

  -- Variación del último cambio de precio registrado
  lp.last_change_percent,
  lp.updated_at                         AS last_price_update,

  -- Semáforo de alerta: ALERTA si el último cambio superó ±10%
  CASE
    WHEN ABS(lp.last_change_percent) > 10 THEN 'ALERTA'
    ELSE 'OK'
  END                                   AS price_alert

FROM
  `project-b8b7f8b6-4ca7-4b6e-96b.dropshipping.final_products`  AS fp

-- Enriquecimiento AI: LEFT JOIN para no perder productos sin categorizar
LEFT JOIN
  `project-b8b7f8b6-4ca7-4b6e-96b.dropshipping.enriched_products` AS ep
  ON fp.product_id = ep.product_id

-- Último precio: filtramos rn = 1 para quedarnos solo con la fila más reciente
LEFT JOIN
  latest_prices AS lp
  ON fp.product_id = lp.product_id
  AND lp.rn = 1;
