# The SDK's one development environment and its checks (README.md,
# "Development"). Every test of the SDK runs with this venv's Python, never
# another interpreter: .venv, made by uv with the Python of .python-version.
UV ?= uv
VENV := .venv
PY := $(VENV)/bin/python

.PHONY: env test lint wheel-check

env: ## The venv, made or brought up to date (ENV_ML=1 adds requirements-ml.txt).
	$(UV) venv --managed-python --python "$$(cat .python-version)" --allow-existing $(VENV)
	$(UV) pip install --python $(PY) -e ".[s3,gcs,seal]" -r requirements-dev.txt \
		$(if $(ENV_ML),-r requirements-ml.txt --index-strategy unsafe-best-match)

test: ## The unit suite, then each skip by its reason.
	ZENTHROS_DEV_ENV=1 PYTHONPATH=tests $(PY) scripts/run_tests.py tests

lint: ## ruff, with pyproject.toml's [tool.ruff].
	$(VENV)/bin/ruff check zenthros tests examples scripts

wheel-check: ## The wheel, installed alone in a venv outside the repository.
	UV=$(UV) $(PY) scripts/wheel_check.py
