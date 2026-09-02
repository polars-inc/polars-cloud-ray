import logging
import os
import subprocess
import urllib.error
import urllib.request

import ray

from polars_onprem_ray.actors.utils import _handle_sigterm, _resolve_host, _stop
from polars_onprem_ray.config import PolarsLicenseServerRuntimeConfig

LICENSE_SERVER_NAME_PREFIX = "license-server"

logger = logging.getLogger(__name__)


def resolve_license_server_name() -> str:
    return LICENSE_SERVER_NAME_PREFIX


@ray.remote
class PolarsLicenseServerActor:
    """The air-gapped offline license validation and reporting service."""

    def __init__(self, config: PolarsLicenseServerRuntimeConfig) -> None:
        self.config: PolarsLicenseServerRuntimeConfig = config

        self.host: str = _resolve_host()

        self._process: subprocess.Popen | None = None

        self.start()
        _handle_sigterm(self.stop)

    def __ray_shutdown__(self) -> None:
        self.stop()

    def start(self) -> None:
        """Spawn the license server process, if not already running."""
        if self._process is not None:
            return

        logger.info("Starting license server on %s", self.host)

        env = {**os.environ, **self.config.env()}

        self._process = subprocess.Popen([self.config.binary_path], env=env)
        logger.info("License server process ID: %d", self._process.pid)

    def stop(self) -> None:
        """Terminate the license server process."""
        _stop(self._process, resolve_license_server_name())
        self._process = None

    def is_ready(self) -> bool:
        """Return whether the gRPC TLS listener is bound and serving."""
        if self._process is None or self._process.poll() is not None:
            return False
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{self.config.http_port}/readyz",
                timeout=0.5,
            ) as response:
                return response.status == 200
        except (urllib.error.URLError, OSError):
            return False

    def get_host(self) -> str:
        """Return the IP address the license server is running on."""
        return self.host

    def get_bind_addr(self) -> str:
        """Return the address of the gRPC service pc-cublet registers/pings against."""
        return f"https://{self.host}:{self.config.grpc_port}"

    def get_metrics_addr(self) -> str:
        """Return the URL Prometheus gauges can be scraped from."""
        return f"http://{self.host}:{self.config.http_port}/metrics"

    def get_license_server_pid(self) -> int | None:
        """Return the OS process ID of the license server binary subprocess."""
        return self._process.pid if self._process is not None else None
