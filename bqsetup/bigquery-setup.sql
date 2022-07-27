CREATE SCHEMA IF NOT EXISTS retail_stage;
DROP TABLE IF EXISTS retail_stage.user_purchase;

CREATE TABLE retail_stage.user_purchase (
    invoice_number STRING(20),
    stock_code STRING(20),
    detail STRING(1000),
    quantity INT,
    invoice_date TIMESTAMP,
    unit_price NUMERIC(8, 3),
    customer_id INT,
    country STRING(20),
    insertion_date TIMESTAMP
)
PARTITION BY
TIMESTAMP_TRUNC(insertion_date, DAY);