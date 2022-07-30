DELETE FROM retail_stage.user_purchase
WHERE TIMESTAMP_TRUNC(insertion_date, DAY) = '{{  execution_date | ds  }}';

LOAD DATA INTO retail_stage.user_purchase 
PARTITION BY TIMESTAMP_TRUNC(insertion_date, DAY) 
FROM FILES 
(
    format = 'CSV',
    skip_leading_rows = 1, 
    uris = ['gs://{{ var.value.get('BUCKET') }}/stage/user_purchase/{{  execution_date | ds  }}.csv']
)