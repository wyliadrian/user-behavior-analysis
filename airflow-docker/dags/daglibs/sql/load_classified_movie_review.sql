DELETE FROM movie_review.classified_movie_review
WHERE TIMESTAMP_TRUNC(insert_date, DAY) = '{{  execution_date | ds }}';

LOAD DATA INTO movie_review.classified_movie_review
PARTITION BY TIMESTAMP_TRUNC(insert_date, DAY)
FROM FILES 
(
  format = 'PARQUET',
  uris = ["gs://{{ var.value.get('BUCKET') }}/stage/movie_review/{{  execution_date | ds }}.parquet/*.parquet"]
);