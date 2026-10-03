# Словарь витрин

Все таблицы находятся в базе analytics, схема public. UTC применяется к timestamptz.
DECIMAL/NUMERIC не преобразуется в float в Python. Суммирование разных валют запрещено.

| Таблица | Гранулярность / PK | Поля / правила |
|---|---|---|
| mart_payments_daily | payment_date, currency, channel | transaction_count BIGINT; total_amount NUMERIC(38,2), только POSTED |
| mart_customer_activity | customer_id, activity_date, currency | transaction_count; incoming_amount и outgoing_amount NUMERIC(38,2); last_event_at TIMESTAMPTZ |
| dim_customer_history | customer_version_id; unique(customer_id,valid_from) | region, segment, valid_from, valid_to, is_current, change_version |

customer_version_id — SHA-256 customer_id и начала интервала. У текущей версии valid_to=NULL.
Исторический сегмент операции определяется интервалом, не текущей строкой справочника.
В течение дня сегмент может измениться: activity агрегирует клиента независимо от сегмента,
а детализацию по историческому сегменту выполняет запрос Iceberg analytics.sql.

PENDING и CANCELLED остаются в DDS, но отсутствуют в обороте. Новая версия операции заменяет
предыдущую целиком. Опубликованная витрина полностью заменяется текущим результатом расчёта,
включая исчезнувшие группы. Статус счёта отражает актуальное состояние; исторического статуса
счёта источник не предоставляет, поэтому текущий CLOSED сам по себе не удаляет прежние операции.

Сравнение с предыдущим **календарным** днём, включая нулевые дни:

```sql
WITH calendar AS (
  SELECT d::date payment_date FROM generate_series('2025-02-01'::date,'2025-02-07'::date,'1 day') d
), currencies AS (
  SELECT DISTINCT currency FROM mart_payments_daily
), totals AS (
  SELECT payment_date,currency,sum(total_amount) turnover FROM mart_payments_daily GROUP BY 1,2
), complete AS (
  SELECT c.payment_date,v.currency,coalesce(t.turnover,0) turnover
  FROM calendar c CROSS JOIN currencies v LEFT JOIN totals t USING(payment_date,currency)
)
SELECT *,turnover-lag(turnover) OVER (PARTITION BY currency ORDER BY payment_date) day_delta,
       sum(turnover) OVER (PARTITION BY currency ORDER BY payment_date) cumulative_turnover
FROM complete;
```

Контроль эксплуатации: batches (состояния и published_at), runs (попытки и snapshot IDs),
stage_metrics (длительность и счётчики, ошибка), reconciliation (суммы и количества по валюте),
alerts (локальные уведомления). Счётчики accepted/rejected в validate считаются по исходным
строкам; accepted в dds — количество текущих уникальных операций, changed — вставленные
или изменённые факты. Эти величины имеют разную гранулярность и не должны складываться.
