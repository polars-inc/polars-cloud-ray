import logging
import os
import pathlib
import subprocess
import tempfile

import ray

from polars_cloud_ray.actors.utils import (
    _handle_sigterm,
    _is_ready,
    _resolve_host,
    _stop,
    _stop_orphans,
    list_actor_names,
    resolve_actor_handles,
    terminate_actors,
)
from polars_cloud_ray.actors.worker import (
    WORKER_NAME_PREFIX,
    PolarsWorkerActor,
    _resolve_worker_name_regex,
    resolve_worker_name,
)
from polars_cloud_ray.config import PolarsRayClusterConfig

SCHEDULER_NAME_PREFIX = "scheduler"

logger = logging.getLogger(__name__)


def resolve_scheduler_name() -> str:
    return SCHEDULER_NAME_PREFIX


@ray.remote
class PolarsSchedulerActor:
    """The central service, distributing tasks and piloting autoscaling."""

    def __init__(self, config: PolarsRayClusterConfig) -> None:
        self.config: PolarsRayClusterConfig = config

        self.scheduler_host: str = _resolve_host()
        self.num_workers: int = config.num_workers  # initial value

        self._config_path: str | None = None
        self._process: subprocess.Popen | None = None

        self.start()
        _handle_sigterm(self.stop)

    def __ray_shutdown__(self) -> None:
        self.stop()

    def _add_worker(self, worker_id: int) -> None:
        actor_name = resolve_worker_name(worker_id)

        PolarsWorkerActor.options(  # type: ignore[attr-defined]
            name=actor_name,
            namespace=self.config.cluster_id,
            lifetime="detached",
            max_restarts=self.config.worker_max_restarts,
            num_cpus=self.config.worker.cpus_hint,
            memory=self.config.worker.memory_hint,
        ).remote(self.config, worker_id, self.scheduler_host)

        logger.info("Requested new worker %s", actor_name)

    def _remove_workers(self, worker_actor_names: set[str]) -> None:
        actors = resolve_actor_handles(worker_actor_names, self.config.cluster_id)
        terminate_actors(list(actors.values()), self.config.actor_response_timeout)

        for actor_name in worker_actor_names:
            logger.info("Removed worker %s", actor_name)

    def start(self) -> None:
        """Spawn the scheduler process, if not already running."""
        if self._process is not None:
            return

        _stop_orphans(
            self.config.binary_path,
            self.config.cluster_id,
            resolve_scheduler_name(),
            [
                self.config.scheduler.worker_registration_port,
                self.config.scheduler.client_port,
            ],
        )

        toml = self.config.config_scheduler(self.scheduler_host)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml)
            self._config_path = f.name

        logger.info(
            "Starting scheduler (%s observatory) on %s",
            "and" if self.config.scheduler.observatory.enabled else "without",
            self.scheduler_host,
        )

        cmd = [self.config.binary_path, "service", "--config-path", self._config_path]
        env = {
            **os.environ,
            "POLARS_EULA_ACCEPTED": "yes" if self.config.accept_eula else "no",
            "PLC_LOG_LEVEL": self.config.log_level,
            "RUST_BACKTRACE": self.config.rust_backtrace,
        }

        self._process = subprocess.Popen(cmd, env=env)
        logger.info("Scheduler process ID: %d", self._process.pid)

    def stop(self) -> None:
        """Terminate the scheduler process and clean up."""
        _stop(self._process, resolve_scheduler_name())
        self._process = None

        if self._config_path is not None:
            pathlib.Path(self._config_path).unlink()
            self._config_path = None

    def is_ready(self) -> bool:
        """Return whether the scheduler process is alive and accepting registration."""
        return _is_ready(
            self._process,
            self.scheduler_host,
            self.config.scheduler.worker_registration_port,
        )

    def get_host(self) -> str:
        """Return the IP address the scheduler is running on."""
        return self.scheduler_host

    def get_client_addr(self) -> str:
        """Return the URI queries should be submitted to."""
        return f"{self.scheduler_host}:{self.config.scheduler.client_port}"

    def get_dashboard_addr(self) -> str | None:
        """Return the observatory dashboard URL."""
        return (
            f"http://{self.scheduler_host}:{self.config.scheduler.observatory.rest_port}"
            if self.config.scheduler.observatory.enabled
            else None
        )

    def get_scheduler_pid(self) -> int | None:
        """Return the OS process ID of the scheduler binary subprocess."""
        return self._process.pid if self._process is not None else None

    def get_scaling_status(self) -> dict[str, int | set[str] | None]:
        """Return the current/desired worker counts and configured bounds."""
        return {
            "available": list_actor_names(self.config.cluster_id, WORKER_NAME_PREFIX),
            "desired": self.num_workers,
            "min": self.config.min_workers,
            "max": self.config.max_workers,
        }

    def rescale_worker_pool_to(
        self,
        num_workers: int,
        delete: set[str] | None = None,
        keep: set[str] | None = None,
    ) -> None:
        """Rescale the number of workers according to the scheduler requests.

        Parameters
        ----------
        num_workers
            The number of worker to upscale or downscale to. If neither `delete` nor
            `keep` is given, random workers are removed.
        delete
            Set of worker instances to terminate. Applied before `keep`.
        keep
            Set of worker instances to keep running, while terminating all the others
            not already removed via `delete`.

        """
        worker_names = list_actor_names(self.config.cluster_id, WORKER_NAME_PREFIX)

        if len(worker_names) < num_workers:
            # offset the id used by each worker to avoid collisions with running worker
            # names
            offset = (
                max(
                    [
                        int(m.group(1))
                        for actor_name in worker_names
                        if (
                            (m := _resolve_worker_name_regex().match(actor_name))
                            is not None
                        )
                    ],
                    default=-1,
                )
                + 1
            )
            for worker_id in range(num_workers - len(worker_names)):
                self._add_worker(worker_id + offset)

        elif len(worker_names) > num_workers:
            if num_workers == 0:
                to_remove = set(worker_names)
            elif delete is None and keep is None:
                to_remove = set(list(worker_names)[: len(worker_names) - num_workers])
            else:
                to_remove = set()
                remaining = worker_names
                if delete is not None:
                    to_remove |= delete
                    remaining = remaining - delete
                if keep is not None:
                    to_remove |= remaining - keep

            self._remove_workers(to_remove)

        self.num_workers = num_workers  # new value
