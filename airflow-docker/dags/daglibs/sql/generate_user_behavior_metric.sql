DELETE FROM `user_behavior.user_behavior_metric`
WHERE insert_date = '{{  execution_date | ds  }}';

INSERT INTO `user_behavior.user_behavior_metric` (
  customer_id,
  amount_spent,
  review_score,
  review_count,
  insert_date
)
SELECT up.customer_id,
       CAST(SUM(up.quantity * up.unit_price) AS NUMERIC) AS amount_spent,
       SUM(cmr.positive_review) AS review_score,
       COUNT(cmr.cid) AS review_count,
       '{{  execution_date | ds  }}'
FROM `retail_stage.user_purchase` up
JOIN (
  SELECT cid,
         CASE
           WHEN positive_review IS TRUE THEN 1
           ELSE 0 END
          AS positive_review
  FROM `movie_review.classified_movie_review`
  WHERE insert_date = '{{  execution_date | ds  }}'
) cmr
ON up.customer_id = cmr.cid
WHERE TIMESTAMP_TRUNC(up.insertion_date, DAY) = '{{  execution_date | ds  }}'
GROUP BY up.customer_id