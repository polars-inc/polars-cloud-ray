import logging
import os
import pathlib
import re
import subprocess
import tempfile

import ray

from polars_onprem_ray.actors.utils import (
    _handle_sigterm,
    _is_ready,
    _resolve_host,
    _stop,
    _stop_orphans,
)
from polars_onprem_ray.config import PolarsRayClusterConfig

WORKER_NAME_PREFIX = "worker"

logger = logging.getLogger(__name__)


def resolve_worker_name(worker_id: int) -> str:
    return f"{WORKER_NAME_PREFIX}-{worker_id}"


def _resolve_worker_name_regex() -> re.Pattern:
    return re.compile(rf"^{WORKER_NAME_PREFIX}-(\d+)$")


@ray.remote
class PolarsWorkerActor:
    """The computing service, accepting tasks from the scheduler."""

    def __init__(
        self,
        config: PolarsRayClusterConfig,
        worker_id: int,
        scheduler_host: str,
    ) -> None:
        self.config: PolarsRayClusterConfig = config

        self.worker_id = worker_id
        self.worker_host: str = _resolve_host()
        self.worker_name: str = ""
        self.scheduler_host = scheduler_host

        self._config_path: str | None = None
        self._process: subprocess.Popen | None = None

        self.start()
        _handle_sigterm(self.stop)

    def __ray_shutdown__(self) -> None:
        self.stop()

    def start(self) -> None:
        """Spawn the worker process, if not already running."""
        if self._process is not None:
            return

        self.worker_name = resolve_worker_name(self.worker_id)
        offset = self.config._worker_port_offset(self.worker_id)

        _stop_orphans(
            self.config.binary_path,
            self.config.cluster_id,
            self.worker_name,
            [
                self.config.worker.task_port + offset,
                self.config.worker.shuffle_port + offset,
            ],
        )

        toml = self.config.config_worker(
            self.worker_id,
            self.worker_host,
            self.scheduler_host,
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml)
            self._config_path = f.name

        logger.info("Starting %s on %s", self.worker_name, self.worker_host)

        cmd = [self.config.binary_path, "service", "--config-path", self._config_path]
        env = {
            **os.environ,
            "POLARS_EULA_ACCEPTED": "yes" if self.config.accept_eula else "no",
            "POLARS_TEMP_DIR": self.config.worker.temporary_data_dir,
            "PLC_LOG_LEVEL": self.config.log_level,
            "RUST_BACKTRACE": self.config.rust_backtrace,
        }

        self._process = subprocess.Popen(cmd, env=env)
        logger.info("PID of %s: %d", self.worker_name, self._process.pid)

    def stop(self) -> None:
        """Terminate the worker process and clean up."""
        _stop(self._process, self.worker_name)
        self._process = None

        if self._config_path is not None:
            pathlib.Path(self._config_path).unlink()
            self._config_path = None

    def is_ready(self) -> bool:
        """Return whether the worker process is alive and listening for tasks."""
        port = self.config.worker.task_port + self.config._worker_port_offset(
            self.worker_id
        )
        return _is_ready(self._process, self.worker_host, port)

    def get_worker_pid(self) -> int | None:
        """Return the OS process ID of the worker binary subprocess."""
        return self._process.pid if self._process is not None else None
