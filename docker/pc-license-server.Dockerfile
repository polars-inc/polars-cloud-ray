FROM python:3.12-slim

COPY --from=binaries pc-license-server /usr/local/bin/pc-license-server

RUN chmod +x /usr/local/bin/pc-license-server

ENTRYPOINT ["pc-license-server"]
