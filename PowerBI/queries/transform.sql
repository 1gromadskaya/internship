INSERT INTO core.fact_sales (order_id, order_date, product_id, customer_id, sales, quantity, profit)
SELECT DISTINCT
    order_id, order_date, product_id, customer_id, sales, quantity, profit
FROM stage.superstore_raw
ON CONFLICT (order_id, product_id) DO NOTHING;

INSERT INTO core.dim_product (product_id, product_name, category, sub_category)
SELECT DISTINCT
    product_id, product_name, category, sub_category
FROM stage.superstore_raw
ON CONFLICT (product_id) DO UPDATE
SET
    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    sub_category = EXCLUDED.sub_category;

UPDATE core.dim_customer target
SET
    is_current = FALSE,
    expiration_date = CURRENT_DATE
FROM stage.superstore_raw src
WHERE target.customer_id = src.customer_id
  AND target.is_current = TRUE
  AND target.region <> src.region;

INSERT INTO core.dim_customer (customer_id, customer_name, segment, region, effective_date, expiration_date, is_current)
SELECT DISTINCT
    src.customer_id,
    src.customer_name,
    src.segment,
    src.region,
    CURRENT_DATE AS effective_date,
    NULL AS expiration_date,
    TRUE AS is_current
FROM stage.superstore_raw src
LEFT JOIN core.dim_customer target
  ON src.customer_id = target.customer_id AND target.is_current = TRUE
WHERE target.customer_id IS NULL
   OR target.region <> src.region;