import logging
import os
import time
import typing

import ray

from polars_onprem_ray.actors import (
    SCALER_NAME,
    SCHEDULER_NAME,
    WORKER_NAME,
    PolarsOnPremScalerActor,
    PolarsOnPremSchedulerActor,
    PolarsOnPremWorkerActor,
    list_actor_names,
)
from polars_onprem_ray.config import PolarsOnPremClusterConfig

logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO").upper())
logger = logging.getLogger(__name__)


class PolarsOnPremCluster:
    """Manage lifecycle of scheduler and workers.

    ```py
    import polars as pl
    import polars_cloud as pc
    import ray

    from polars_onprem_ray.cluster import PolarsOnPremCluster
    from polars_onprem_ray.config import (
        PolarsOnPremClusterConfig,
        PolarsOnPremEnterpriseLicenseConfig,
        PolarsOnPremSchedulerConfig,
        PolarsOnPremWorkerConfig,
    )

    config = PolarsOnPremClusterConfig(
        # single_host_cluster=True,
        num_workers=4,
        license=PolarsOnPremEnterpriseLicenseConfig(license_path="./license.json"),
        scheduler=PolarsOnPremSchedulerConfig(cpu_max=1, memory_max=2 * 1024**3),
        worker=PolarsOnPremWorkerConfig(cpu_max=2, memory_max=4 * 1024**3),
    )

    ray.init(address="auto", namespace=config.cluster_id)
    cluster = PolarsOnPremCluster(config)
    cluster.start()

    print(
        pl.LazyFrame({"a": [1, 2, 3], "b": [4, 4, 5]})
        .with_columns(pl.col("a").max().over("b").alias("c"))
        .remote(pc.ClusterContext(uri=f"http://{cluster.get_client_addr()}"))
        .execute()
        .head
    )

    cluster.stop()
    ray.shutdown()
    ```
    """

    def __init__(self, config: PolarsOnPremClusterConfig) -> None:
        self.config = config

        self._scaler_actor: typing.Any = None
        self._scheduler_actor: typing.Any = None
        self._worker_actors: list[typing.Any] = []

    def _start_scheduler(self) -> None:
        try:
            self._scheduler_actor = ray.get_actor(
                SCHEDULER_NAME,
                namespace=self.config.cluster_id,
            )
        except ValueError:
            pass
        else:
            logger.info("Reconnected to existing scheduler actor")
            return

        self._scheduler_actor = PolarsOnPremSchedulerActor.options(  # type: ignore[attr-defined]
            name=SCHEDULER_NAME,
            namespace=self.config.cluster_id,
            lifetime="detached",
            resources={"head": 0.001},  # pinning
            num_cpus=self.config.scheduler.cpu_max,
            memory=self.config.scheduler.memory_max,
        ).remote(self.config)

    def _start_workers(self) -> None:
        if self._scheduler_actor is None:
            return

        scheduler_host = ray.get(self._scheduler_actor.get_host.remote())

        worker_actors: list[typing.Any] = []

        for worker_id in range(self.config.num_workers):
            actor_name = f"{WORKER_NAME}-{worker_id}"

            try:
                actor = ray.get_actor(actor_name, namespace=self.config.cluster_id)
                logger.info("Reconnected to existing worker %s", actor_name)
            except ValueError:
                actor = PolarsOnPremWorkerActor.options(  # type: ignore[attr-defined]
                    name=actor_name,
                    namespace=self.config.cluster_id,
                    lifetime="detached",
                    max_restarts=self.config.worker_max_restarts,
                    num_cpus=self.config.worker.cpu_max,
                    memory=self.config.worker.memory_max,
                ).remote(self.config, worker_id, scheduler_host)
                logger.info("Worker %s started", actor_name)

            worker_actors.append(actor)

        self._worker_actors = worker_actors

    def _start_scaler(self) -> None:
        try:
            self._scaler_actor = ray.get_actor(
                SCALER_NAME,
                namespace=self.config.cluster_id,
            )
        except ValueError:
            pass
        else:
            logger.info("Reconnected to existing scaler actor")
            return

        self._scaler_actor = PolarsOnPremScalerActor.options(  # type: ignore[attr-defined]
            name=SCALER_NAME,
            namespace=self.config.cluster_id,
            lifetime="detached",
            resources={"head": 0.001},  # pinning, same node as the scheduler
            num_cpus=0,
        ).remote(self.config)

    def _wait_for_scheduler(self) -> None:
        logger.info(
            "Waiting up to %ds for the scheduler",
            self.config.worker_startup_timeout,
        )

        deadline = time.monotonic() + self.config.worker_startup_timeout
        while time.monotonic() < deadline:
            if ray.get(self._scheduler_actor.is_ready.remote()):
                logger.info("Scheduler started and listening")
                return
            time.sleep(2)

        msg = f"Scheduler did not start within {self.config.worker_startup_timeout}s"
        raise RuntimeError(msg)

    def _wait_for_workers(self) -> None:
        logger.info(
            "Waiting up to %ds for %d workers",
            self.config.worker_startup_timeout,
            len(self._worker_actors),
        )

        pending: dict[int, typing.Any] = dict(enumerate(self._worker_actors))

        deadline = time.monotonic() + self.config.worker_startup_timeout
        while pending and time.monotonic() < deadline:
            readiness = ray.get([actor.is_ready.remote() for actor in pending.values()])
            for worker_id, worker_ready in zip(
                list(pending.keys()), readiness, strict=True
            ):
                if worker_ready:
                    logger.info("Worker %s is ready", f"{WORKER_NAME}-{worker_id}")
                    pending.pop(worker_id)
            if pending:
                time.sleep(2)

        if pending:
            msg = (
                f"Workers {[f'{WORKER_NAME}-{worker_id}' for worker_id in pending]} "
                f"did not start within {self.config.worker_startup_timeout}s"
            )
            raise RuntimeError(msg)

    def _wait_for_scaler(self) -> None:
        logger.info(
            "Waiting up to %ds for the scaler",
            self.config.worker_startup_timeout,
        )

        deadline = time.monotonic() + self.config.worker_startup_timeout
        while time.monotonic() < deadline:
            if ray.get(self._scaler_actor.is_ready.remote()):
                logger.info("Scaler actor started and listening")
                return
            time.sleep(2)

        msg = f"Scaler did not start within {self.config.worker_startup_timeout}s"
        raise RuntimeError(msg)

    # this should probably never be called directly
    def _rescale_worker_pool_to(
        self,
        num_workers: int,
        delete: set[str] | None = None,
        keep: set[str] | None = None,
    ) -> None:
        if self._scheduler_actor is None:
            return
        ray.get(
            self._scheduler_actor.rescale_worker_pool_to.remote(
                num_workers,
                delete=delete,
                keep=keep,
            )
        )

    def start(self) -> None:
        """Start (or reconnect to) our various actors; block until ready."""
        if self.config.scheduler.scaling.enabled:
            self._start_scaler()
            self._wait_for_scaler()

        self._start_scheduler()
        self._start_workers()

        self._wait_for_scheduler()
        self._wait_for_workers()

        msg = (
            f"Engine cluster ready: scheduler at {self.get_client_addr()}, "
            f"{len(self._worker_actors)} workers."
        )
        if self.config.scheduler.observatory.enabled:
            msg += f" Dashboard available at {self.get_dashboard_addr()}."
        logger.info(msg)

    def stop(self) -> None:
        """Terminate all the actors (scaler, scheduler, workers)."""
        if self.config.scheduler.scaling.enabled:
            try:
                self._scaler_actor = ray.get_actor(
                    SCALER_NAME, namespace=self.config.cluster_id
                )
            except ValueError:
                self._scaler_actor = None

            if self._scaler_actor is not None:
                logger.info("Stopping scaler...")
                ray.get(self._scaler_actor.stop.remote())
                ray.kill(self._scaler_actor)
                self._scaler_actor = None

        self._worker_actors = [
            ray.get_actor(name, namespace=self.config.cluster_id)
            for name in list_actor_names(self.config.cluster_id, WORKER_NAME)
        ]
        if len(self._worker_actors):
            logger.info("Stopping %d worker(s)...", len(self._worker_actors))
            ray.get([actor.stop.remote() for actor in self._worker_actors])
            for actor in self._worker_actors:
                ray.kill(actor)
            self._worker_actors = []

        try:
            self._scheduler_actor = ray.get_actor(
                SCHEDULER_NAME, namespace=self.config.cluster_id
            )
        except ValueError:
            self._scheduler_actor = None

        if self._scheduler_actor is not None:
            logger.info("Stopping scheduler...")
            ray.get(self._scheduler_actor.stop.remote())
            ray.kill(self._scheduler_actor)
            self._scheduler_actor = None

        logger.info("Cluster stopped")

    def get_client_addr(self) -> str:
        """Return the URI queries should be submitted to."""
        return (
            str(ray.get(self._scheduler_actor.get_client_addr.remote()))
            if self._scheduler_actor is not None
            else "The scheduler does not appear to be running"
        )

    def get_dashboard_addr(self) -> str:
        """Return the observatory dashboard URL."""
        return (
            str(ray.get(self._scheduler_actor.get_dashboard_addr.remote()))
            if (
                self._scheduler_actor is not None
                and self.config.scheduler.observatory.enabled
            )
            else "The observatory is not enabled on this cluster"
        )
