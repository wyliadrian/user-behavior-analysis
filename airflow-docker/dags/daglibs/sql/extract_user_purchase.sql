COPY (
       SELECT 
           invoice_number,
           stock_code,
           detail,
           quantity,
           invoice_date,
           unit_price,
           customer_id,
           country,
           CURRENT_TIMESTAMP AS insertion_date
        FROM retail.user_purchase
        WHERE EXTRACT(DAY FROM invoice_date) = {{  execution_date.day  }}
        AND EXTRACT(MONTH FROM invoice_date) = {{  execution_date.month  }}
) TO '/var/lib/postgresql/data/temp/{{  execution_date | ds }}.csv' WITH (FORMAT CSV, HEADER);