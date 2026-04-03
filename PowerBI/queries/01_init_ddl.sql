CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS core.dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    customer_name VARCHAR(100),
    segment VARCHAR(50),
    effective_date DATE,
    expiration_date DATE,
    is_current BOOLEAN
);