.PHONY: install run test lint charts
install:  ## install Python dependencies (needs Java 17+ for Spark)
	pip install -r requirements.txt
run:      ## ingest sample -> Spark ETL -> DuckDB queries (no AWS needed)
	python scripts/run_local.py && python scripts/make_charts.py
test:
	python -m pytest -q
lint:
	cfn-lint infra/template.yaml
