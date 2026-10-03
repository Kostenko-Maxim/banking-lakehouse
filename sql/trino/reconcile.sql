SELECT currency,count(*) transaction_count,cast(sum(amount) AS decimal(38,2)) total_amount
FROM iceberg.dds.transactions WHERE status='POSTED' GROUP BY currency
