# For Postgres
CREATE SCHEMA IF NOT EXISTS retail_stage;

CREATE TABLE IF NOT EXISTS retail_stage.user_purchase (
    invoice_number varchar(10),
    stock_code varchar(20),
    detail varchar(1000),
    quantity int,
    invoice_date timestamp,
    unit_price Numeric(8, 3),
    customer_id int,
    country varchar(20),
    insertion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COPY retail_stage.user_purchase(invoice_number, stock_code, detail, quantity, 
                                invoice_date, unit_price, customer_id, country)
FROM '/var/lib/postgresql/data/stage/{{  execution_date | ds  }}.csv' DELIMITER ',' CSV HEADER;
