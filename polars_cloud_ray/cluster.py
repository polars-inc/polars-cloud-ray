import logging
import os
import time
import typing

import ray
from ray.exceptions import GetTimeoutError

from polars_cloud_ray.actors import (
    WORKER_NAME_PREFIX,
    PolarsScalerActor,
    PolarsSchedulerActor,
    PolarsWorkerActor,
    list_actor_names,
    resolve_actor_handles,
    resolve_scaler_name,
    resolve_scheduler_name,
    resolve_worker_name,
    terminate_actors,
)
from polars_cloud_ray.config import PolarsRayClusterConfig

logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO").upper())
logger = logging.getLogger(__name__)


class PolarsRayCluster:
    """Manage the lifecycles of the scheduler, workers and scaler actors.

    ```py
    import polars as pl
    import polars_cloud as pc
    import ray

    from polars_cloud_ray.cluster import PolarsRayCluster
    from polars_cloud_ray.config import (
        PolarsObservatoryConfig,
        PolarsRayClusterConfig,
        PolarsSchedulerConfig,
        PolarsServiceAccountLicenseConfig,
    )

    config = PolarsRayClusterConfig(
        # single_host_cluster=True,
        num_workers=4,
        license=PolarsServiceAccountLicenseConfig(
            client_id="<SERVICE_ACCOUNT_ID>",
            client_secret="<SERVICE_ACCOUNT_SECRET>",
        ),
        scheduler=PolarsSchedulerConfig(
            observatory=PolarsObservatoryConfig(
                database_path="/tmp/polars/observatory"
            ),
        ),
    )

    ray.init(address="auto", namespace=config.cluster_id)
    cluster = PolarsRayCluster(config)
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

    def __init__(self, config: PolarsRayClusterConfig) -> None:
        self.config = config

        self._scaler_actor: typing.Any = None
        self._scheduler_actor: typing.Any = None
        self._worker_actors: list[typing.Any] = []

    def _actor_exists(self, name: str) -> bool:
        try:
            ray.get_actor(name, namespace=self.config.cluster_id)
        except ValueError:
            return False
        return True

    def _start_scheduler(self) -> None:
        actor_name = resolve_scheduler_name()

        try:
            self._scheduler_actor = ray.get_actor(
                actor_name,
                namespace=self.config.cluster_id,
            )
        except ValueError:
            pass
        else:
            logger.info("Reconnected to existing scheduler actor")
            return

        self._scheduler_actor = PolarsSchedulerActor.options(  # type: ignore[attr-defined]
            name=actor_name,
            namespace=self.config.cluster_id,
            lifetime="detached",
            resources={"head": 0.001},  # pinning
            num_cpus=self.config.scheduler.cpus_hint,
            memory=self.config.scheduler.memory_hint,
        ).remote(self.config)

    def _start_workers(self) -> None:
        if self._scheduler_actor is None:
            msg = "Cannot start workers: scheduler actor is not available."
            raise RuntimeError(msg)

        scheduler_host = ray.get(
            self._scheduler_actor.get_host.remote(),
            timeout=self.config.actor_response_timeout,
        )

        worker_actors: list[typing.Any] = []

        for worker_id in range(self.config.num_workers):
            actor_name = resolve_worker_name(worker_id)

            try:
                actor = ray.get_actor(actor_name, namespace=self.config.cluster_id)
                logger.info("Reconnected to existing worker %s", actor_name)
            except ValueError:
                actor = PolarsWorkerActor.options(  # type: ignore[attr-defined]
                    name=actor_name,
                    namespace=self.config.cluster_id,
                    lifetime="detached",
                    max_restarts=self.config.worker_max_restarts,
                    num_cpus=self.config.worker.cpus_hint,
                    memory=self.config.worker.memory_hint,
                ).remote(self.config, worker_id, scheduler_host)
                logger.info("Worker %s started", actor_name)

            worker_actors.append(actor)

        self._worker_actors = worker_actors

    def _start_scaler(self) -> None:
        actor_name = resolve_scaler_name()

        try:
            self._scaler_actor = ray.get_actor(
                actor_name,
                namespace=self.config.cluster_id,
            )
        except ValueError:
            pass
        else:
            logger.info("Reconnected to existing scaler actor")
            return

        self._scaler_actor = PolarsScalerActor.options(  # type: ignore[attr-defined]
            name=actor_name,
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
        while (remaining := deadline - time.monotonic()) > 0:
            try:
                if ray.get(self._scheduler_actor.is_ready.remote(), timeout=remaining):
                    logger.info("Scheduler started and listening")
                    return
            except GetTimeoutError:
                break
            time.sleep(0.5)

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
        while pending and (remaining := deadline - time.monotonic()) > 0:
            try:
                readiness = ray.get(
                    [actor.is_ready.remote() for actor in pending.values()],
                    timeout=remaining,
                )
            except GetTimeoutError:
                break

            for worker_id, worker_ready in zip(
                list(pending.keys()),
                readiness,
                strict=True,
            ):
                if worker_ready:
                    logger.info("Worker %s is ready", resolve_worker_name(worker_id))
                    pending.pop(worker_id)

            if pending:
                time.sleep(0.5)

        if pending:
            msg = (
                f"Workers {[resolve_worker_name(worker_id) for worker_id in pending]} "
                f"did not start within {self.config.worker_startup_timeout}s"
            )
            raise RuntimeError(msg)

    def _wait_for_scaler(self) -> None:
        logger.info(
            "Waiting up to %ds for the scaler",
            self.config.worker_startup_timeout,
        )

        deadline = time.monotonic() + self.config.worker_startup_timeout
        while (remaining := deadline - time.monotonic()) > 0:
            try:
                if ray.get(self._scaler_actor.is_ready.remote(), timeout=remaining):
                    logger.info("Scaler actor started and listening")
                    return
            except GetTimeoutError:
                break
            time.sleep(0.5)

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
            ),
            timeout=self.config.actor_response_timeout,
        )

    def start(self) -> None:
        """Start (or reconnect to) our various actors; block until ready."""
        self.config._port_availability(
            check_scheduler=not self._actor_exists(resolve_scheduler_name()),
            check_workers=[
                worker_id
                for worker_id in range(self.config.num_workers)
                if not self._actor_exists(resolve_worker_name(worker_id))
            ],
            check_scaler=not self._actor_exists(resolve_scaler_name()),
        )

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
        try:
            self._scheduler_actor = ray.get_actor(
                resolve_scheduler_name(),
                namespace=self.config.cluster_id,
            )
        except ValueError:
            self._scheduler_actor = None

        if self.config.scheduler.scaling.enabled:
            try:
                self._scaler_actor = ray.get_actor(
                    resolve_scaler_name(),
                    namespace=self.config.cluster_id,
                )
            except ValueError:
                self._scaler_actor = None

            if self._scaler_actor is not None:
                logger.info("Stopping scaler...")
                terminate_actors(
                    [self._scaler_actor],
                    self.config.actor_response_timeout,
                )
                self._scaler_actor = None

        worker_names = list_actor_names(self.config.cluster_id, WORKER_NAME_PREFIX)
        self._worker_actors = list(
            resolve_actor_handles(worker_names, self.config.cluster_id).values()
        )
        if self._worker_actors:
            logger.info("Stopping %d worker(s)...", len(self._worker_actors))
            terminate_actors(self._worker_actors, self.config.actor_response_timeout)
            self._worker_actors = []

        if self._scheduler_actor is not None:
            logger.info("Stopping scheduler...")
            terminate_actors(
                [self._scheduler_actor],
                self.config.actor_response_timeout,
            )
            self._scheduler_actor = None

        logger.info("Cluster stopped")

    def get_client_addr(self) -> str:
        """Return the URI queries should be submitted to."""
        if self._scheduler_actor is None:
            msg = "The scheduler does not appear to be running."
            raise RuntimeError(msg)

        return str(
            ray.get(
                self._scheduler_actor.get_client_addr.remote(),
                timeout=self.config.actor_response_timeout,
            )
        )

    def get_dashboard_addr(self) -> str:
        """Return the observatory dashboard URL."""
        if self._scheduler_actor is None:
            msg = "The scheduler does not appear to be running."
            raise RuntimeError(msg)

        if not self.config.scheduler.observatory.enabled:
            return "The observatory is not enabled on this cluster"

        return str(
            ray.get(
                self._scheduler_actor.get_dashboard_addr.remote(),
                timeout=self.config.actor_response_timeout,
            )
        )
