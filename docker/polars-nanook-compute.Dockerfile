ARG PYTHON_VERSION=3.12

FROM python:${PYTHON_VERSION}-slim

ARG COMPUTE_VERSION

RUN --mount=from=ghcr.io/astral-sh/uv:0.12,source=/uv,target=/usr/local/bin/uv \
    uv pip install --no-cache-dir --system polars-nanook-compute==$COMPUTE_VERSION

ENTRYPOINT ["polars-nanook-compute"]
