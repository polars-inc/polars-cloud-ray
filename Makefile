SHELL=/bin/bash

BINARY_PATH="$(PWD)/polars-on-premises"
LICENSE_PATH="$(PWD)/license.json"

.venv:
	@uv venv --python=3.12
	@uv pip install --requirement=requirements-dev.txt

test:
	@BINARY_PATH=$(BINARY_PATH) LICENSE_PATH=$(LICENSE_PATH) uv run python -m pytest --verbose

clean:
	@rm -fr .pytest_cache .ropeproject .ruff_cache uv.lock .venv
	@rm -fr $$(find . -name __pycache__)
