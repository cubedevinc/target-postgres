SHELL=bash

.PHONY: test
test: .develop
	pytest --cov target_postgres --cov-report html target_postgres/ $(PYTEST_ARGS)

.PHONY: lint
lint: .develop
	pylint -E target_postgres/ $(PYLINT_ARGS)

.PHONY: dev
dev: 
	python -m venv venv
	./venv/bin/pip install -e .[dev]

.develop: setup.py
	python setup.py develop easy_install target-postgres[dev]
