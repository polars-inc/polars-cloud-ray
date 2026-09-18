import logging
import os
import time
import typing

import ray
from ray.exceptions import GetTimeoutError

from polars_cloud_ray.actors import (
    PolarsLicenseServerActor,
    resolve_license_server_name,
    terminate_actors,
)
from polars_cloud_ray.config import PolarsLicenseServerRuntimeConfig

logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO").upper())
logger = logging.getLogger(__name__)


class PolarsLicenseServer:
    """Manage the lifecycle of the standalone license server actor.

    ```py
    import ray

    from polars_cloud_ray.config import PolarsLicenseServerRuntimeConfig
    from polars_cloud_ray.license_server import PolarsLicenseServer

    config = PolarsLicenseServerRuntimeConfig(
        report_dir="/var/log/polars/license-server",
        license_path="/etc/polars/license.json",
        tls_bundle_path="/etc/polars/tls-bundle.pem",
    )

    ray.init(address="auto", namespace="polars-onprem-license-server")
    license_server = PolarsLicenseServer(config)
    license_server.start()

    print(license_server.get_bind_addr())

    license_server.stop()
    ray.shutdown()
    ```
    """

    def __init__(self, config: PolarsLicenseServerRuntimeConfig) -> None:
        self.config = config

        self._actor: typing.Any = None

    def _start_actor(self) -> None:
        actor_name = resolve_license_server_name()

        try:
            self._actor = ray.get_actor(actor_name)
        except ValueError:
            pass
        else:
            logger.info("Reconnected to existing license server actor")
            return

        self._actor = PolarsLicenseServerActor.options(  # type: ignore[attr-defined]
            name=actor_name,
            lifetime="detached",
            resources={"head": 0.001},  # pinning
            num_cpus=self.config.cpu_max,
            memory=self.config.memory_max,
        ).remote(self.config)

    def _wait_for_actor(self) -> None:
        logger.info(
            "Waiting up to %ds for the license server",
            self.config.startup_timeout,
        )

        deadline = time.monotonic() + self.config.startup_timeout
        while (remaining := deadline - time.monotonic()) > 0:
            try:
                if ray.get(self._actor.is_ready.remote(), timeout=remaining):
                    logger.info("License server started and listening")
                    return
            except GetTimeoutError:
                break
            time.sleep(0.5)

        msg = f"License server did not start within {self.config.startup_timeout}s"
        raise RuntimeError(msg)

    def start(self) -> None:
        """Start (or reconnect to) the license server actor; block until ready."""
        self._start_actor()
        self._wait_for_actor()

        logger.info("License server ready at %s", self.get_bind_addr())

    def stop(self) -> None:
        """Terminate the license server actor."""
        try:
            self._actor = ray.get_actor(resolve_license_server_name())
        except ValueError:
            self._actor = None

        if self._actor is not None:
            logger.info("Stopping license server...")
            terminate_actors([self._actor], self.config.actor_response_timeout)
            self._actor = None

        logger.info("License server stopped")

    def get_host(self) -> str:
        """Return the IP address the license server is running on."""
        if self._actor is None:
            msg = "The license server does not appear to be running."
            raise RuntimeError(msg)

        return str(
            ray.get(
                self._actor.get_host.remote(),
                timeout=self.config.actor_response_timeout,
            )
        )

    def get_bind_addr(self) -> str:
        """Return the address of the gRPC service pc-cublet registers/pings against."""
        if self._actor is None:
            msg = "The license server does not appear to be running."
            raise RuntimeError(msg)

        return str(
            ray.get(
                self._actor.get_bind_addr.remote(),
                timeout=self.config.actor_response_timeout,
            )
        )

    def get_metrics_addr(self) -> str:
        """Return the URL Prometheus gauges can be scraped from."""
        if self._actor is None:
            msg = "The license server does not appear to be running."
            raise RuntimeError(msg)

        return str(
            ray.get(
                self._actor.get_metrics_addr.remote(),
                timeout=self.config.actor_response_timeout,
            )
        )
