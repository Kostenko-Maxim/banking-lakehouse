COMPOSE = docker compose
DATE ?= 2025-01-01
PROFILE ?= small
REPEATS ?= 3

.PHONY: bootstrap up generate demo test integration-test backfill benchmark down reset status schema-demo maintenance
bootstrap:
	test -f .env || cp .env.example .env
	$(COMPOSE) config --quiet
	$(COMPOSE) build namenode spark-master
	$(COMPOSE) build metastore airflow-init
	$(COMPOSE) pull postgres trino db-init
up:
	$(COMPOSE) up -d --wait --wait-timeout 600
generate:
	$(COMPOSE) exec -T airflow-scheduler python -m bank.cli generate --date $(DATE) --profile $(PROFILE)
demo:
	$(COMPOSE) exec -T airflow-scheduler python -m bank.cli demo
test:
	python -m pytest -q
integration-test:
	python scripts/integration.py
backfill:
	$(COMPOSE) exec -T airflow-scheduler python -m bank.cli backfill --start $(START) --end $(END)
benchmark:
	$(COMPOSE) exec -T airflow-scheduler python -m bank.cli benchmark --date $(DATE) --profile $(PROFILE) --repeats $(REPEATS)
schema-demo:
	$(COMPOSE) exec -T airflow-scheduler python -m bank.cli schema-demo
maintenance:
	$(COMPOSE) exec -T airflow-scheduler python -m bank.cli maintenance
status:
	$(COMPOSE) ps
	$(COMPOSE) stats --no-stream
down:
	$(COMPOSE) down
reset:
	@printf 'DELETE ALL banking-lakehouse volumes? Type DELETE: '; read answer; test "$$answer" = DELETE
	$(COMPOSE) down --volumes
