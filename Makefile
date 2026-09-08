.PHONY: start stop restart logs env workload-image workload-setup history cdc generate cdc-drain cdc-test workload-check

WORKLOAD_COMPOSE := docker compose -f docker-compose.yml -f compose.workload.yml

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

workload-image:
	$(WORKLOAD_COMPOSE) --profile workload build workload

workload-setup: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.setup

history: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.history

cdc: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.cdc

generate: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.generate

cdc-drain: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.cdc --drain

cdc-test: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.smoke_test

workload-check: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.verify
