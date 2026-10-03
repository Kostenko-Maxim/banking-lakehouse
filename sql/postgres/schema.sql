CREATE TABLE IF NOT EXISTS pipeline_lock (
    id integer PRIMARY KEY CHECK (id=1), owner text NOT NULL, acquired_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id text PRIMARY KEY, processing_date date NOT NULL, manifest jsonb NOT NULL,
    state text NOT NULL, published_at timestamptz, error text
);
CREATE TABLE IF NOT EXISTS runs (
    run_id text PRIMARY KEY, batch_id text NOT NULL REFERENCES batches, started_at timestamptz DEFAULT now(),
    finished_at timestamptz, state text NOT NULL, error text
);
CREATE TABLE IF NOT EXISTS stage_metrics (
    run_id text REFERENCES runs, stage text, duration_seconds numeric, read_count bigint DEFAULT 0,
    accepted_count bigint DEFAULT 0, rejected_count bigint DEFAULT 0, changed_count bigint DEFAULT 0,
    error text, recorded_at timestamptz DEFAULT now(), PRIMARY KEY(run_id,stage)
);
CREATE TABLE IF NOT EXISTS alerts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, created_at timestamptz DEFAULT now(),
    kind text NOT NULL, payload jsonb NOT NULL
);
ALTER TABLE runs ADD COLUMN IF NOT EXISTS snapshots jsonb;
CREATE TABLE IF NOT EXISTS mart_payments_daily (
    payment_date date, currency text, channel text, transaction_count bigint NOT NULL,
    total_amount numeric(38,2) NOT NULL, PRIMARY KEY(payment_date,currency,channel)
);
CREATE TABLE IF NOT EXISTS mart_customer_activity (
    customer_id text, activity_date date, currency text, transaction_count bigint NOT NULL,
    incoming_amount numeric(38,2) NOT NULL, outgoing_amount numeric(38,2) NOT NULL,
    last_event_at timestamptz NOT NULL, PRIMARY KEY(customer_id,activity_date,currency)
);
CREATE TABLE IF NOT EXISTS dim_customer_history (
    customer_version_id text PRIMARY KEY, customer_id text NOT NULL, region text NOT NULL, segment text NOT NULL,
    valid_from timestamptz NOT NULL, valid_to timestamptz, is_current boolean NOT NULL,
    change_version bigint NOT NULL, CHECK (valid_to IS NULL OR valid_to>valid_from),
    CHECK (is_current=(valid_to IS NULL)), UNIQUE(customer_id,valid_from)
);
CREATE UNIQUE INDEX IF NOT EXISTS one_current_customer ON dim_customer_history(customer_id) WHERE is_current;
CREATE TABLE IF NOT EXISTS stage_payments (LIKE mart_payments_daily INCLUDING DEFAULTS);
ALTER TABLE stage_payments ADD COLUMN IF NOT EXISTS run_id text;
CREATE TABLE IF NOT EXISTS stage_activity (LIKE mart_customer_activity INCLUDING DEFAULTS);
ALTER TABLE stage_activity ADD COLUMN IF NOT EXISTS run_id text;
CREATE TABLE IF NOT EXISTS stage_history (LIKE dim_customer_history INCLUDING DEFAULTS);
ALTER TABLE stage_history ADD COLUMN IF NOT EXISTS run_id text;
CREATE TABLE IF NOT EXISTS reconciliation (
    run_id text, currency text, expected_count bigint, expected_amount numeric(38,2),
    actual_count bigint, actual_amount numeric(38,2), PRIMARY KEY(run_id,currency)
);
