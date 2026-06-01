# Makefile for SAS → dbt/Databricks Migration Project
#
# Common development targets for building, linting, and testing.
# In the SAS world, there was no build system — programs were run
# interactively or scheduled via Control-M. This Makefile provides
# a standardized developer workflow.

.PHONY: install lint lint-fix compile parse test reconcile run run-staging run-intermediate run-marts ci clean help \
        seed teardown build demo-up demo-down deploy deploy-prod run-job destroy

DBT_DIR := dbt_project
SQLFLUFF_CONFIG := .sqlfluff

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## Install dbt, sqlfluff, and pre-commit hooks
	pip install dbt-core dbt-databricks sqlfluff pre-commit yamllint
	pre-commit install
	cd $(DBT_DIR) && dbt deps

lint: ## Run sqlfluff linter on all models
	sqlfluff lint $(DBT_DIR)/models/ --config $(SQLFLUFF_CONFIG) --ignore templating,parsing

lint-fix: ## Auto-fix sqlfluff lint violations
	sqlfluff fix $(DBT_DIR)/models/ --config $(SQLFLUFF_CONFIG) --ignore templating,parsing --force

compile: ## Compile dbt models (requires Databricks connection)
	cd $(DBT_DIR) && dbt compile --target dev

parse: ## Parse/validate dbt project (no connection required)
	cd $(DBT_DIR) && dbt parse --target dev

test: ## Run dbt schema tests (requires Databricks connection)
	cd $(DBT_DIR) && dbt test --target dev

reconcile: ## Source→target reconciliation report for namespace NS (requires Databricks connection)
	python verify/reconcile.py --namespace $(NS)

run-staging: ## Run staging models only
	cd $(DBT_DIR) && dbt run --select tag:staging

run-intermediate: ## Run intermediate models only
	cd $(DBT_DIR) && dbt run --select tag:intermediate

run-marts: ## Run mart models only
	cd $(DBT_DIR) && dbt run --select tag:marts

run: ## Run all dbt models in layer order (staging → intermediate → marts)
	cd $(DBT_DIR) && dbt run --select tag:staging
	cd $(DBT_DIR) && dbt run --select tag:intermediate
	cd $(DBT_DIR) && dbt run --select tag:marts

ci: lint parse ## Run full CI pipeline locally (lint + parse)
	@echo ""
	@echo "CI checks passed. To run integration tests, set DATABRICKS_* env vars and run: make test"

clean: ## Remove dbt build artifacts
	rm -rf $(DBT_DIR)/target $(DBT_DIR)/dbt_packages $(DBT_DIR)/logs

# ---------------------------------------------------------------------------
# Repeatable demo lifecycle (isolated, concurrent-safe DB namespaces)
#
# NS is the schema prefix for a run. Outputs land in <NS>_staging/_intermediate/
# _marts/_curated, so multiple runs (NS=dev, NS=alice, ...) never collide and the
# "before" raw data in banking_analytics.raw is never touched.
#   make demo-up NS=alice     # seed (idempotent) + build that namespace
#   make demo-down NS=alice   # drop only that namespace's schemas
# Requires DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN.
# ---------------------------------------------------------------------------
NS ?= dev
TARGET ?= dev

seed: ## Seed synthetic "before" raw data into banking_analytics.raw (idempotent)
	python seed/generate_and_load.py

teardown: ## Drop one namespace's output schemas (NS=...); raw data untouched
	python seed/teardown.py --namespace $(NS)

build: ## Build + test all models into namespace NS (DBT_SCHEMA=$(NS))
	cd $(DBT_DIR) && DBT_SCHEMA=$(NS) dbt build --target dev

demo-up: seed ## Full "after" state for namespace NS: seed + build + test
	cd $(DBT_DIR) && DBT_SCHEMA=$(NS) dbt build --target dev

demo-down: teardown ## Tear down namespace NS (alias for teardown)

deploy: ## Deploy the Asset Bundle to dev (namespaced per-user, schedule paused)
	databricks bundle deploy -t dev

deploy-prod: ## Deploy the Asset Bundle to the prod target
	databricks bundle deploy -t prod

run-job: ## Trigger the deployed pipeline job (TARGET=dev|prod)
	databricks bundle run daily_banking_pipeline -t $(TARGET)

destroy: ## Revert CD: remove the deployed bundle/job (TARGET=dev|prod)
	databricks bundle destroy -t $(TARGET) --auto-approve
