import logging
import os
import typing
import urllib.error
import urllib.request

if typing.TYPE_CHECKING:
    import pathlib

import ray

from polars_cloud_ray.actors.runtime import SubprocessRunMixin
from polars_cloud_ray.actors.utils import _handle_sigterm
from polars_cloud_ray.config import PolarsLicenseServerRuntimeConfig

LICENSE_SERVER_NAME_PREFIX = "license-server"

logger = logging.getLogger(__name__)


def resolve_license_server_name() -> str:
    return LICENSE_SERVER_NAME_PREFIX


class _PolarsLicenseServerActor(SubprocessRunMixin):
    """Binary lifecycle management; inherit this to run the binary differently."""

    def __init__(self, config: PolarsLicenseServerRuntimeConfig) -> None:
        self.config_service: PolarsLicenseServerRuntimeConfig = config

        self.host: str = self._host(resolve_license_server_name())

        self._handle = None
        self._config_paths: set[pathlib.Path] = self.config_service.config_paths()

        self.start()
        _handle_sigterm(self.stop)

    def __ray_shutdown__(self) -> None:
        self.stop()

    def start(self) -> None:
        """Spawn the license server process, if not already running."""
        if self._handle is not None:
            return

        logger.info("Starting license server on %s", self.host)

        env = {**os.environ, **self.config_service.config()}

        self._start(
            [self.config_service.binary_path],
            env,
            self.config_service.instance_name(resolve_license_server_name()),
        )

    def stop(self) -> None:
        """Terminate the license server process."""
        self._stop(self.config_service.instance_name(resolve_license_server_name()))

    def is_ready(self) -> bool:
        """Return whether the gRPC TLS listener is bound and serving."""
        if not self._is_alive():
            return False
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{self.config_service.http_port}/readyz",
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
        return f"https://{self.host}:{self.config_service.grpc_port}"

    def get_metrics_addr(self) -> str:
        """Return the URL Prometheus gauges can be scraped from."""
        return f"http://{self.host}:{self.config_service.http_port}/metrics"

    def get_license_server_pid(self) -> int | None:
        """Return the OS process ID of the license server binary subprocess."""
        return self._pid()


@ray.remote
class PolarsLicenseServerActor(_PolarsLicenseServerActor):
    """The air-gapped offline license validation and reporting service."""
