.PHONY: start stop restart logs env lint workload-image workload-setup history cdc generate cdc-drain cdc-test workload-check workload-check-history workload-check-cdc

WORKLOAD_COMPOSE := docker compose -f docker-compose.yml -f compose.workload.yml
RUFF := UV_CACHE_DIR=.uv-cache UV_TOOL_DIR=.uv-cache/tools uvx --from ruff==0.16.6 ruff

# Required entry point on first start: Lakekeeper's image has no shell and
# requires a one-shot metadata migration before its server can become healthy.
start:
	@test -f .env || (echo 'Create .env from .env.example and replace the placeholders first.'; exit 1)
	docker compose up -d --wait postgres-source postgres-airflow postgres-catalog minio
	docker compose run --rm --no-deps lakekeeper migrate
	docker compose up -d --wait --wait-timeout 300
	python3 docker/lakekeeper/bootstrap.py

stop:
	docker compose stop

restart:
	docker compose stop
	$(MAKE) start

logs:
	docker compose logs -f --tail=100

env:
	uv venv .venv
	uv pip sync requirements.txt --python .venv/bin/python

lint:
	$(RUFF) format --check workload docker/lakekeeper/bootstrap.py
	$(RUFF) check workload docker/lakekeeper/bootstrap.py

workload-image:
	$(WORKLOAD_COMPOSE) --profile workload build workload

workload-setup: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.cybermarket.bootstrap

history: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.pipelines.history

cdc: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.pipelines.cdc

generate: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.pipelines.generator

cdc-drain: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.pipelines.cdc --drain

cdc-test: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.validation.smoke

workload-check: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.validation.state --phase setup

workload-check-history: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.validation.state --phase history

workload-check-cdc: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.validation.state --phase cdc
