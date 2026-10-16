import json
import subprocess
import time
import urllib.request

import pytest
import ray

from polars_cloud_ray.actors import (
    SCALER_NAME_PREFIX,
    SCHEDULER_NAME_PREFIX,
    WORKER_NAME_PREFIX,
    list_actor_names,
)

from .conftest import USE_DOCKER, RayClusterFactory, TestQuery


def _wait_for_workers(
    cluster_id: str,
    expected: int,
    timeout: float = 30,
) -> set[str]:
    deadline = time.monotonic() + timeout
    names: set[str] = set()

    while time.monotonic() < deadline:
        names = list_actor_names(cluster_id, WORKER_NAME_PREFIX)
        if len(names) == expected:
            return names
        time.sleep(1)

    msg = f"Expected {expected} workers, got {sorted(names)} after {timeout}s"
    raise AssertionError(msg)


def test_upscale(ray_cluster: RayClusterFactory) -> None:
    cluster = ray_cluster(num_workers=1)
    cluster_id = cluster.config.cluster_id

    names = _wait_for_workers(cluster_id, 1)
    assert names == {f"{WORKER_NAME_PREFIX}-{i}" for i in range(1)}

    cluster._rescale_worker_pool_to(2)

    names = _wait_for_workers(cluster_id, 2)
    assert names == {f"{WORKER_NAME_PREFIX}-{i}" for i in range(2)}


def test_downscale_delete(ray_cluster: RayClusterFactory) -> None:
    cluster = ray_cluster(num_workers=2)
    cluster_id = cluster.config.cluster_id

    before = _wait_for_workers(cluster_id, 2)
    delete = set(list(before)[:1])
    cluster._rescale_worker_pool_to(1, delete=delete)
    after = _wait_for_workers(cluster_id, 1)

    assert after == before - delete


def test_downscale_keep(ray_cluster: RayClusterFactory) -> None:
    cluster = ray_cluster(num_workers=2)
    cluster_id = cluster.config.cluster_id

    before = _wait_for_workers(cluster_id, 2)
    keep = set(list(before)[:1])
    cluster._rescale_worker_pool_to(1, keep=keep)
    after = _wait_for_workers(cluster_id, 1)

    assert after == keep


def test_downscale_to_zero(ray_cluster: RayClusterFactory) -> None:
    cluster = ray_cluster(num_workers=2)
    cluster_id = cluster.config.cluster_id

    assert len(_wait_for_workers(cluster_id, 2)) == 2

    cluster._rescale_worker_pool_to(0)
    assert len(_wait_for_workers(cluster_id, 0)) == 0


def test_worker_id_offset(ray_cluster: RayClusterFactory) -> None:
    cluster = ray_cluster(num_workers=2)
    cluster_id = cluster.config.cluster_id

    names = _wait_for_workers(cluster_id, 2)
    assert names == {f"{WORKER_NAME_PREFIX}-{i}" for i in range(2)}

    # remove first only, and scale back up; the new worker must not reuse the terminated
    # worker's id
    cluster._rescale_worker_pool_to(1, keep={f"{WORKER_NAME_PREFIX}-1"})
    _wait_for_workers(cluster_id, 1)

    cluster._rescale_worker_pool_to(2)

    names = _wait_for_workers(cluster_id, 2)
    assert names == {f"{WORKER_NAME_PREFIX}-{i}" for i in range(1, 3)}


def test_scaling_disabled(ray_cluster: RayClusterFactory) -> None:
    cluster = ray_cluster(
        scaling_enabled=False,
        num_workers=1,
    )

    with pytest.raises(ValueError, match=SCALER_NAME_PREFIX):
        ray.get_actor(SCALER_NAME_PREFIX, namespace=cluster.config.cluster_id)


@pytest.mark.parametrize("max_workers", [8, None])
def test_scale_config(
    ray_cluster: RayClusterFactory,
    max_workers: int | None,
) -> None:
    cluster = ray_cluster(
        scaling_enabled=True,
        num_workers=1,
        min_workers=0,
        max_workers=max_workers,
    )

    host = cluster.config.runtime.scaler_host
    port = cluster.config.scheduler.scaling.port
    url = f"http://{host}:{port}/scale_config"

    if USE_DOCKER:
        body = json.loads(
            subprocess.run(
                [
                    "docker",
                    "exec",
                    cluster.config.instance_name(SCHEDULER_NAME_PREFIX),
                    "python",
                    "-c",
                    "import sys, urllib.request; "
                    "print(urllib.request.urlopen(sys.argv[1]).read().decode())",
                    url,
                ],
                capture_output=True,
                text=True,
                check=True,
            ).stdout
        )
    else:
        with urllib.request.urlopen(url) as response:
            body = json.loads(response.read())

    assert body["desired"] == 1
    assert body["available"] == 1
    assert body["min"] == 0
    assert body["max"] == max_workers


def test_client_autoscaling(
    ray_cluster: RayClusterFactory,
    run_query: TestQuery,
) -> None:
    cluster = ray_cluster(
        scaling_enabled=True,
        num_workers=0,
    )

    run_query(cluster=cluster, min_workers=2, max_workers=2)

    assert len(list_actor_names(cluster.config.cluster_id, WORKER_NAME_PREFIX)) == 2
