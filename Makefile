SHELL=/bin/bash

BINARY_PATH="$(PWD)/_bin/pc-cublet"
BINARY_PATH_LICENSE_SERVER="$(PWD)/_bin/pc-license-server"

LICENSE_PATH="$(PWD)/_etc/license.json"
TLS_BUNDLE_PATH="$(PWD)/_etc/tls-bundle.pem"

.venv:
	@uv venv --python=3.12
	@uv pip install --requirement=requirements-dev.txt

test:
	@BINARY_PATH=$(BINARY_PATH) \
	 LICENSE_PATH=$(LICENSE_PATH) \
	 .venv/bin/pytest --verbose

test-license-server:
	@BINARY_PATH=$(BINARY_PATH) \
	 BINARY_PATH_LICENSE_SERVER=$(BINARY_PATH_LICENSE_SERVER) \
	 LICENSE_PATH=$(LICENSE_PATH) \
	 TLS_BUNDLE_PATH=$(TLS_BUNDLE_PATH) \
	 .venv/bin/pytest --verbose tests/integration/test_license_server.py

clean:
	@rm -fr .pytest_cache .ropeproject .ruff_cache uv.lock .venv
	@rm -fr $$(find . -name __pycache__)
