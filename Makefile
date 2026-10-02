# Pulse: common tasks. Run `make help` for a list.
SHELL := /bin/bash
export PULSE_DATA ?= $(CURDIR)/data
export DUCKDB_PATH ?= $(CURDIR)/pulse.duckdb
PY := python

.PHONY: help install up down generate stream stream-once gold report test fixture dbt-fixture tf-plan clean

help:            ## show targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

install:         ## install package with Spark, dbt and dev extras
	$(PY) -m pip install -e ".[spark,dbt,dev]"

up:              ## start Kafka, Prometheus, Grafana, Airflow
	docker compose up -d

down:            ## stop containers
	docker compose down -v

generate:        ## send synthetic clickstream to Kafka (20 sessions/s for 10 min)
	$(PY) -m pulse.generator --rate 20 --seconds 600

stream:          ## run the Spark streaming job (local parquet sink, metrics on :8000)
	$(PY) -m pulse.jobs.stream --sink local --out $(PULSE_DATA)

stream-once:     ## process what is in Kafka now, then stop
	$(PY) -m pulse.jobs.stream --sink local --out $(PULSE_DATA) --once

gold:            ## dbt build (models + tests) on the local data
	cd dbt && dbt build --profiles-dir .

report:          ## A/B significance test on the current marts
	$(PY) -c "from pulse.warehouse import variant_summary; from pulse.abtest import two_proportion_test as t; \
	s=variant_summary().set_index('variant'); \
	print(t(int(s.loc['control','conversions']), int(s.loc['control','sessions']), \
	        int(s.loc['treatment','conversions']), int(s.loc['treatment','sessions'])).summary())"

test:            ## unit tests (Spark/Airflow tests skip if those packages are missing)
	$(PY) -m pytest -q

fixture:         ## synthetic raw parquet in ./data_fixture (no Kafka/Spark needed)
	$(PY) -m pulse.fixtures --out $(CURDIR)/data_fixture

dbt-fixture: fixture   ## dbt build + tests on the fixture
	cd dbt && PULSE_DATA=$(CURDIR)/data_fixture DUCKDB_PATH=$(CURDIR)/fixture.duckdb dbt build --profiles-dir .

tf-plan:         ## terraform plan for GCP (needs GCP_PROJECT)
	cd infra/gcp && terraform init && terraform plan -var="project_id=$(GCP_PROJECT)"

clean:           ## remove local data, checkpoints, dbt artifacts
	rm -rf data data_fixture checkpoints *.duckdb dbt/target dbt/logs
