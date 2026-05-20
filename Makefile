# Makefile for SAS → dbt/Databricks Migration Project
#
# Common development targets for building, linting, and testing.
# In the SAS world, there was no build system — programs were run
# interactively or scheduled via Control-M. This Makefile provides
# a standardized developer workflow.

.PHONY: install lint lint-fix compile test run run-staging run-intermediate run-marts ci clean help

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

compile: ## Compile dbt models (syntax validation, no connection required)
	cd $(DBT_DIR) && dbt compile --target dev

test: ## Run dbt schema tests (requires Databricks connection)
	cd $(DBT_DIR) && dbt test --target dev

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

ci: lint compile ## Run full CI pipeline locally (lint + compile)
	@echo ""
	@echo "CI checks passed. To run integration tests, set DATABRICKS_* env vars and run: make test"

clean: ## Remove dbt build artifacts
	rm -rf $(DBT_DIR)/target $(DBT_DIR)/dbt_packages $(DBT_DIR)/logs
