SHELL=/bin/bash

PYTHON_VERSION?=3.12

BINARY_PATH="$(PWD)/_bin/pc-cublet"
BINARY_PATH_LICENSE_SERVER="$(PWD)/_bin/pc-license-server"

LICENSE_PATH="$(PWD)/_etc/license.json"
TLS_BUNDLE_PATH="$(PWD)/_etc/tls-bundle.pem"

.venv:
	@uv venv --python=$(PYTHON_VERSION) --managed-python
	@uv pip install --requirement=requirements-dev.txt
	@uv pip install --editable .

test: .venv
	@source .venv/bin/activate && \
	 BINARY_PATH=$(BINARY_PATH) \
	 LICENSE_PATH=$(LICENSE_PATH) \
	 pytest --verbose

test-license-server: .venv
	@source .venv/bin/activate && \
	 BINARY_PATH=$(BINARY_PATH) \
	 BINARY_PATH_LICENSE_SERVER=$(BINARY_PATH_LICENSE_SERVER) \
	 LICENSE_PATH=$(LICENSE_PATH) \
	 TLS_BUNDLE_PATH=$(TLS_BUNDLE_PATH) \
	 pytest --verbose tests/integration/test_license_server.py

clean:
	@rm -fr .pytest_cache .ropeproject .ruff_cache uv.lock .venv
	@rm -fr $$(find . -name __pycache__)
