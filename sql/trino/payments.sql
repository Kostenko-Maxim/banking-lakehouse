SELECT cast(event_at AS date) payment_date, currency, channel,
       count(*) transaction_count, cast(sum(amount) AS decimal(38,2)) total_amount
FROM iceberg.dds.transactions
WHERE status='POSTED'
GROUP BY 1,2,3
