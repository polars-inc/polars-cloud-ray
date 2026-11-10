SHELL=/bin/bash

PYTHON_VERSION?=3.12
COMPUTE_VERSION?=0.10.1

BINARY_PATH_LICENSE_SERVER="$(PWD)/_bin/polars-nanook-license-server"

CONTAINER_IMAGE="polars-nanook-ray:latest"
CONTAINER_IMAGE_LICENSE_SERVER="polars-nanook-ray-license-server:latest"

LICENSE_PATH="$(PWD)/_etc/license.json"
TLS_BUNDLE_PATH="$(PWD)/_etc/tls-bundle.pem"

.venv:
	@uv venv --python=$(PYTHON_VERSION) --managed-python
	@uv pip install --requirement=requirements-dev.txt
	@uv pip install --editable .

image:
	@docker buildx build \
	  --build-arg COMPUTE_VERSION=$(COMPUTE_VERSION) \
	  --build-arg PYTHON_VERSION=$(PYTHON_VERSION) \
	  --file ./docker/polars-nanook-compute.Dockerfile \
	  --tag $(CONTAINER_IMAGE) \
	  .

image-license-server:
	@docker buildx build \
	  --build-context binaries=$$(dirname $(BINARY_PATH_LICENSE_SERVER)) \
	  --file ./docker/polars-nanook-license-server.Dockerfile \
	  --tag $(CONTAINER_IMAGE_LICENSE_SERVER) \
	  .

test-docker: .venv
	@source .venv/bin/activate && \
	 PYTEST_USE_DOCKER=1 \
	 CONTAINER_IMAGE=$(CONTAINER_IMAGE) \
	 LICENSE_PATH=$(LICENSE_PATH) \
	 pytest --verbose

test-docker-license-server: .venv
	@source .venv/bin/activate && \
	 PYTEST_USE_DOCKER=1 \
	 CONTAINER_IMAGE=$(CONTAINER_IMAGE) \
	 CONTAINER_IMAGE_LICENSE_SERVER=$(CONTAINER_IMAGE_LICENSE_SERVER) \
	 LICENSE_PATH=$(LICENSE_PATH) \
	 TLS_BUNDLE_PATH=$(TLS_BUNDLE_PATH) \
	 pytest --verbose tests/integration/test_license_server.py

test-subprocess: .venv
	@source .venv/bin/activate && \
	 LICENSE_PATH=$(LICENSE_PATH) \
	 pytest --verbose

test-license-server-subprocess: .venv
	@source .venv/bin/activate && \
	 BINARY_PATH_LICENSE_SERVER=$(BINARY_PATH_LICENSE_SERVER) \
	 LICENSE_PATH=$(LICENSE_PATH) \
	 TLS_BUNDLE_PATH=$(TLS_BUNDLE_PATH) \
	 pytest --verbose tests/integration/test_license_server.py

test: test-subprocess

test-license-server: test-license-server-subprocess

clean:
	@rm -fr .mypy_cache .pytest_cache .ropeproject .ruff_cache uv.lock .venv
	@rm -fr $$(find . -name __pycache__)
