-- Историческое соединение: сегмент на момент операции, включая позднее изменение истории.
WITH historical AS (
  SELECT t.*,h.region,h.segment
  FROM iceberg.dds.transactions t
  JOIN iceberg.dds.customer_history h ON t.customer_id=h.customer_id
   AND t.event_at>=h.valid_from AND (h.valid_to IS NULL OR t.event_at<h.valid_to)
  WHERE t.status='POSTED'
), daily AS (
  SELECT customer_id,cast(event_at AS date) payment_date,currency,
         array_agg(DISTINCT segment) historical_segments,sum(amount) turnover
  FROM historical GROUP BY 1,2,3
)
SELECT *,sum(turnover) OVER (PARTITION BY customer_id,currency ORDER BY payment_date
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_turnover,
         turnover-lag(turnover) OVER (PARTITION BY customer_id,currency ORDER BY payment_date) previous_active_day_delta,
         dense_rank() OVER (PARTITION BY payment_date,currency ORDER BY turnover DESC) customer_rank
FROM daily
