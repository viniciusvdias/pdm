.PHONY: install test benchmark report run-all qa qa-check

GRAPH_RAW_PATH ?= data/raw/soc-pokec-relationships.txt

install:
	pip install -e ".[dev]"

test:
	pytest tests/unit/ -v

benchmark:
	python -m cli.main benchmark --input "$(GRAPH_RAW_PATH)" --fractions 100 --runs 3

report:
	python -m cli.main report

run-all: benchmark report

docker:
	bash scripts/run-docker.sh

qa:
	@echo "Running full QA suite (informative, never aborts)..."
	bash scripts/run_qa.sh

qa-check:
	@echo "Running fast lint checks..."
	ruff check src/ || true
	pylint src/ --score=y || true
	bandit -r src/ -q || true
