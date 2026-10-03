SELECT customer_id, cast(event_at AS date) activity_date, currency,
       count(*) transaction_count,
       cast(sum(CASE WHEN direction='IN' THEN amount ELSE decimal '0.00' END) AS decimal(38,2)) incoming_amount,
       cast(sum(CASE WHEN direction='OUT' THEN amount ELSE decimal '0.00' END) AS decimal(38,2)) outgoing_amount,
       max(event_at) last_event_at
FROM iceberg.dds.transactions
WHERE status='POSTED'
GROUP BY 1,2,3
