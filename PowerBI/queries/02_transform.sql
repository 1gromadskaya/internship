DROP VIEW IF EXISTS mart.v_fact_sales CASCADE;

CREATE VIEW mart.v_fact_sales AS
SELECT
    f.order_id,
    f.order_date,
    c.customer_name,
    c.segment,
    p.category,
    p.sub_category,
    p.product_name,
    g.region,
    f.sales,
    f.quantity,
    f.profit
FROM core.fact_sales f
LEFT JOIN core.dim_customer c
    ON f.customer_sk = c.customer_sk AND c.is_current = TRUE
LEFT JOIN core.dim_product p
    ON f.product_id = p.product_id
LEFT JOIN core.dim_geography g
    ON f.geo_id = g.geo_id;