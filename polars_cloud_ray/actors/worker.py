import logging
import os
import pathlib
import re
import tempfile
import typing

import ray

from polars_cloud_ray.actors.runtime import SubprocessRunMixin
from polars_cloud_ray.actors.utils import _handle_sigterm, _raise_if_exited
from polars_cloud_ray.config import PolarsRayClusterConfig

if typing.TYPE_CHECKING:
    from polars_cloud_ray.config import PolarsWorkerConfig

WORKER_NAME_PREFIX = "worker"

logger = logging.getLogger(__name__)


def resolve_worker_name(worker_id: int) -> str:
    return f"{WORKER_NAME_PREFIX}-{worker_id}"


def _resolve_worker_name_regex() -> re.Pattern:
    return re.compile(rf"^{WORKER_NAME_PREFIX}-(\d+)$")


class _PolarsWorkerActor(SubprocessRunMixin):
    """Binary lifecycle management; inherit this to run the binary differently."""

    def __init__(
        self,
        config: PolarsRayClusterConfig,
        worker_id: int,
        scheduler_host: str,
    ) -> None:
        self.config_cluster: PolarsRayClusterConfig = config
        self.config_service: PolarsWorkerConfig = config.worker

        self.worker_id = worker_id
        self.worker_host: str = self._host(resolve_worker_name(worker_id))
        self.worker_name: str = resolve_worker_name(worker_id)
        self.scheduler_host = scheduler_host

        self._config_path: str | None = None
        self._config_paths: set[pathlib.Path] = (
            self.config_cluster.config_paths_worker()
        )
        self._handle = None

        self.start()
        _handle_sigterm(self.stop)

    def __ray_shutdown__(self) -> None:
        self.stop()

    def start(self) -> None:
        """Spawn the worker process, if not already running."""
        if self._handle is not None:
            return

        offset = self.config_cluster._worker_port_offset(self.worker_id)

        self._stop_orphans(
            self.worker_name,
            [
                self.config_cluster.worker.task_port + offset,
                self.config_cluster.worker.shuffle_port + offset,
            ],
        )

        toml = self.config_cluster.config_worker(
            self.worker_id,
            self.worker_host,
            self.scheduler_host,
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml)
            self._config_path = f.name

        logger.info("Starting %s on %s", self.worker_name, self.worker_host)

        cmd = [
            self.config_cluster.binary_path,
            "service",
            "--config-path",
            self._config_path,
        ]
        env = {
            **os.environ,
            "POLARS_EULA_ACCEPTED": "yes" if self.config_cluster.accept_eula else "no",
            "POLARS_TEMP_DIR": self.config_cluster.worker.temporary_data_dir,
            "PLC_LOG_LEVEL": self.config_cluster.log_level,
            "RUST_BACKTRACE": self.config_cluster.rust_backtrace,
        }

        self._start(
            cmd,
            env,
            self.config_cluster.instance_name(resolve_worker_name(self.worker_id)),
        )

    def stop(self) -> None:
        """Terminate the worker process and clean up."""
        self._stop(
            self.config_cluster.instance_name(resolve_worker_name(self.worker_id))
        )

        if self._config_path is not None:
            pathlib.Path(self._config_path).unlink()
            self._config_path = None

    def is_ready(self) -> bool:
        """Return whether the worker process is alive and listening for tasks."""
        _raise_if_exited(resolve_worker_name(self.worker_id), self._exit_code())

        return self._is_alive() and self._is_listening(
            resolve_worker_name(self.worker_id),
            (
                self.config_cluster.worker.task_port
                + self.config_cluster._worker_port_offset(self.worker_id)
            ),
        )

    def get_worker_pid(self) -> int | None:
        """Return the OS process ID of the worker binary subprocess."""
        return self._pid()


@ray.remote
class PolarsWorkerActor(_PolarsWorkerActor):
    """The computing service, accepting tasks from the scheduler."""
