FROM python:3.12-slim

ARG POLARS_VERSION

COPY --from=binaries pc-cublet /usr/local/bin/pc-cublet

RUN --mount=from=ghcr.io/astral-sh/uv:0.12,source=/uv,target=/usr/local/bin/uv \
    uv pip install --no-cache-dir --system polars[cloudpickle,rt64]==$POLARS_VERSION && \
    chmod +x /usr/local/bin/pc-cublet

ENTRYPOINT ["pc-cublet"]
