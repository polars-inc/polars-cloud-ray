FROM python:3.12-slim

COPY --from=binaries polars-nanook-license-server /usr/local/bin/polars-nanook-license-server

RUN chmod +x /usr/local/bin/polars-nanook-license-server

ENTRYPOINT ["polars-nanook-license-server"]
