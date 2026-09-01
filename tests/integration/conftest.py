import os
import pathlib
import shutil
import socket
import subprocess
import sysconfig
import uuid
from collections.abc import Callable, Iterator

import polars as pl
import polars_cloud as pc
import pytest
import ray

from polars_onprem_ray.cluster import PolarsRayCluster
from polars_onprem_ray.config import (
    PolarsEnterpriseLicenseConfig,
    PolarsMonitoringConfig,
    PolarsObservatoryConfig,
    PolarsRayClusterConfig,
    PolarsScalingConfig,
    PolarsSchedulerConfig,
    PolarsWorkerConfig,
)
from polars_onprem_ray.context import RayClusterContext

RayClusterConfigFactory = Callable[..., PolarsRayClusterConfig]
RayClusterFactory = Callable[..., PolarsRayCluster]
TestQuery = Callable[..., pl.DataFrame | None]

RAY_GCS_PORT = 6379
RAY_CLIENT_PORT = 10001


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _free_port_range(max_num_workers: int = 10) -> int:
    for _ in range(33):
        sockets = [socket.socket(socket.AF_INET, socket.SOCK_STREAM)]
        sockets[0].bind(("127.0.0.1", 0))
        port = sockets[0].getsockname()[1]

        is_range_free = True
        for i in range(1, max_num_workers):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.bind(("127.0.0.1", port + i * 2))
            except OSError:
                is_range_free = False
                sock.close()
                break
            sockets.append(sock)

        for sock in sockets:
            sock.close()

        if is_range_free:
            return port

    msg = f"Could not find a free port range of size {max_num_workers}"
    raise RuntimeError(msg)


def _ld_library_path() -> str:
    libdir = sysconfig.get_config_var("LIBDIR")
    ld_library_path = os.environ.get("LD_LIBRARY_PATH")
    return f"{ld_library_path}:{libdir}" if ld_library_path else libdir


def _python_path() -> str:
    purelib = sysconfig.get_path("purelib")
    python_path = os.environ.get("PYTHONPATH")
    return f"{python_path}:{purelib}" if python_path else purelib


@pytest.fixture(scope="session", autouse=True)
def ray_head() -> Iterator[None]:
    """Start a detached Ray head node for the session."""
    os.environ["LD_LIBRARY_PATH"] = _ld_library_path()
    os.environ["PYTHONPATH"] = _python_path()

    if (binary_path := shutil.which("ray")) is None:
        msg = "Ray is not installed or not in PATH!"
        raise RuntimeError(msg)

    for name in ("anonymous-results", "shuffle-data", "temporary-data"):
        pathlib.Path("/tmp/polars", name).mkdir(parents=True, exist_ok=True)

    subprocess.run([binary_path, "stop", "--force"], check=False)  # clean slate

    subprocess.run(
        [
            binary_path,
            "start",
            "--head",
            "--dashboard-host=0.0.0.0",
            "--disable-usage-stats",
            f"--port={RAY_GCS_PORT}",
            f"--ray-client-server-port={RAY_CLIENT_PORT}",
            '--resources={"head":1}',
        ],
        check=True,
    )

    yield

    subprocess.run([binary_path, "stop", "--force"], check=False)


@pytest.fixture
def ray_cluster(
    ray_cluster_config: RayClusterConfigFactory,
) -> Iterator[RayClusterFactory]:
    started: list[PolarsRayCluster] = []

    def _factory(**kwargs) -> PolarsRayCluster:
        config = ray_cluster_config(**kwargs)
        ray.init(address="auto", namespace=config.cluster_id, ignore_reinit_error=True)
        cluster = PolarsRayCluster(config)
        started.append(cluster)
        cluster.start()
        return cluster

    yield _factory

    for cluster in started:
        cluster.stop()

    if ray.is_initialized():
        ray.shutdown()


@pytest.fixture
def ray_cluster_config() -> RayClusterConfigFactory:
    def _factory(
        *,
        # cluster configuration
        worker_max_restarts: int = 3,
        worker_memory_limit: int | None = None,
        # native binary configuration
        scaling_enabled: bool = False,
        num_workers: int = 0,
        min_workers: int = 0,
        max_workers: int | None = None,
    ) -> PolarsRayClusterConfig:
        cluster_id = f"polars-onprem-{uuid.uuid4().hex[:8]}"

        binary_path = os.environ.get("BINARY_PATH", "./pc-cublet")
        license_path = os.environ.get("LICENSE_PATH", "./license.json")

        return PolarsRayClusterConfig(
            # cluster configuration
            binary_path=binary_path,
            single_host_cluster=True,
            worker_max_restarts=worker_max_restarts,
            # native binary configuration
            cluster_id=cluster_id,
            num_workers=num_workers,
            min_workers=min_workers,
            max_workers=max_workers,
            license=PolarsEnterpriseLicenseConfig(license_path=license_path),
            scheduler=PolarsSchedulerConfig(
                # cluster configuration
                cpus_hint=1,
                memory_hint=1,
                # native binary configuration
                cpu_reserved=1,
                client_port=_free_port(),
                worker_registration_port=_free_port(),
                observatory=PolarsObservatoryConfig(enabled=False),
                scaling=PolarsScalingConfig(
                    enabled=scaling_enabled,
                    port=_free_port(),
                ),
            ),
            worker=PolarsWorkerConfig(
                # cluster configuration
                cpus_hint=1,
                memory_hint=1,
                # native binary configuration
                cpu_reserved=1,
                memory_limit=worker_memory_limit,
                task_port=_free_port_range(),
                shuffle_port=_free_port_range(),
            ),
            monitoring=PolarsMonitoringConfig(enabled=False),
        )

    return _factory


@pytest.fixture
def run_query() -> TestQuery:
    def _factory(
        *,
        cluster: PolarsRayCluster | None = None,
        context: RayClusterContext | None = None,
        min_workers: int | None = None,
        max_workers: int | None = None,
    ) -> pl.DataFrame | None:
        if cluster is not None:
            ctx = pc.ClusterContext(uri=f"http://{cluster.get_client_addr()}")
        elif context is not None:
            ctx = context
        else:
            msg = "No cluster or context provided"
            raise ValueError(msg)

        query = (
            pl.LazyFrame({"a": [1, 2, 3], "b": [4, 4, 5]})
            .with_columns(pl.col("a").max().over("b").alias("c"))
            .remote(ctx)
        )

        if min_workers is not None or max_workers is not None:
            query = query.distributed(min_workers=min_workers, max_workers=max_workers)

        return query.execute().head

    return _factory
