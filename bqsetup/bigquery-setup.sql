CREATE SCHEMA IF NOT EXISTS retail_stage;
DROP TABLE IF EXISTS retail_stage.user_purchase;

CREATE TABLE retail_stage.user_purchase (
    invoice_number STRING(20),
    stock_code STRING(20),
    detail STRING(1000),
    quantity INT,
    invoice_date TIMESTAMP,
    unit_price NUMERIC(8, 3),
    customer_id STRING,
    country STRING(20),
    insertion_date TIMESTAMP
)
PARTITION BY
TIMESTAMP_TRUNC(insertion_date, DAY);


CREATE SCHEMA IF NOT EXISTS movie_review;
DROP TABLE IF EXISTS movie_review.classified_movie_review;

CREATE TABLE movie_review.classified_movie_review (
    customer_id STRING,
    positive_review BOOLEAN,
    insert_date TIMESTAMP
)
PARTITION BY
TIMESTAMP_TRUNC(insert_date, DAY);


CREATE SCHEMA IF NOT EXISTS user_behavior;
DROP TABLE IF EXISTS user_behavior.user_behavior_metric;

CREATE TABLE user_behavior.user_behavior_metric (
    customer_id STRING,
    amount_spent NUMERIC(18, 5),
    review_score INT,
    review_count INT,
    insert_date STRING
);