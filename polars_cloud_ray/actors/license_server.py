import logging
import os
import urllib.error
import urllib.request

import ray

from polars_cloud_ray.actors.runtimes import resolve_runner
from polars_cloud_ray.actors.utils import _handle_sigterm
from polars_cloud_ray.config import PolarsRayLicenseServerConfig

LICENSE_SERVER_NAME_PREFIX = "license-server"

logger = logging.getLogger(__name__)


def resolve_license_server_name() -> str:
    return LICENSE_SERVER_NAME_PREFIX


@ray.remote
class PolarsLicenseServerActor:
    """The air-gapped offline license validation and reporting service."""

    def __init__(self, config: PolarsRayLicenseServerConfig) -> None:
        self.config_service: PolarsRayLicenseServerConfig = config

        name = resolve_license_server_name()
        self._runner = resolve_runner(
            config.runtime,
            name,
            config.instance_name(name),
            cpus=config.cpus_hint,
            memory=config.memory_hint,
            mounts_rw=config.config_paths_rw(),
            mounts_ro=config.config_paths_ro(),
            ports=config.config_ports(),
        )

        self.host: str = self._runner.host

        self.start()
        _handle_sigterm(self.stop)

    def __ray_shutdown__(self) -> None:
        self.stop()

    def start(self) -> None:
        """Spawn the license server process, if not already running."""
        if self._runner.is_started():
            return

        logger.info("Starting license server on %s", self.host)

        args: list[str] = []
        env: dict[str, str] = {**os.environ, **self.config_service.config()}

        self._runner.start(args, env)

    def stop(self) -> None:
        """Terminate the license server process."""
        self._runner.stop()

    def is_ready(self) -> bool:
        """Return whether the gRPC TLS listener is bound and serving."""
        self._runner._raise_if_exited()

        if not self._runner.is_alive():
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
        host = self.host
        port = self.config_service.grpc_port
        return f"https://{host}:{port}"

    def get_metrics_addr(self) -> str:
        """Return the URL Prometheus gauges can be scraped from."""
        host = self.config_service.runtime.scheduler_host or self.host
        port = self.config_service.http_port
        return f"http://{host}:{port}/metrics"

    def get_license_server_pid(self) -> int | None:
        """Return the OS process ID of the license server binary subprocess."""
        return self._runner.pid()
