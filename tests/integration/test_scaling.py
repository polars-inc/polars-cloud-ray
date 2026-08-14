import json
import time
import urllib.request

import pytest
import ray
from polars_onprem_ray.actors import SCALER_NAME, WORKER_NAME, list_actor_names

from .conftest import RayClusterFactory, TestQuery


def _wait_for_workers(
    cluster_id: str,
    expected: int,
    timeout: float = 20,
) -> set[str]:
    deadline = time.monotonic() + timeout
    names: set[str] = set()

    while time.monotonic() < deadline:
        names = list_actor_names(cluster_id, WORKER_NAME)
        if len(names) == expected:
            return names
        time.sleep(1)

    msg = f"Expected {expected} workers, got {sorted(names)} after {timeout}s"
    raise AssertionError(msg)


def test_upscale(ray_cluster: RayClusterFactory) -> None:
    cluster = ray_cluster(num_workers=2)
    cluster_id = cluster.config.cluster_id

    names = _wait_for_workers(cluster_id, 2)
    assert names == {f"{WORKER_NAME}-{i}" for i in range(2)}

    cluster._rescale_worker_pool_to(4)

    names = _wait_for_workers(cluster_id, 4)
    assert names == {f"{WORKER_NAME}-{i}" for i in range(4)}


def test_downscale_delete(ray_cluster: RayClusterFactory) -> None:
    cluster = ray_cluster(num_workers=4)
    cluster_id = cluster.config.cluster_id

    before = _wait_for_workers(cluster_id, 4)
    delete = set(list(before)[:2])
    cluster._rescale_worker_pool_to(2, delete=delete)
    after = _wait_for_workers(cluster_id, 2)

    assert after == before - delete


def test_downscale_keep(ray_cluster: RayClusterFactory) -> None:
    cluster = ray_cluster(num_workers=4)
    cluster_id = cluster.config.cluster_id

    before = _wait_for_workers(cluster_id, 4)
    keep = set(list(before)[:2])
    cluster._rescale_worker_pool_to(2, keep=keep)
    after = _wait_for_workers(cluster_id, 2)

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
    assert names == {f"{WORKER_NAME}-{i}" for i in range(2)}

    # remove first only, and scale back up; the new worker must not reuse the terminated
    # worker's id
    cluster._rescale_worker_pool_to(1, keep={f"{WORKER_NAME}-1"})
    _wait_for_workers(cluster_id, 1)

    cluster._rescale_worker_pool_to(2)

    names = _wait_for_workers(cluster_id, 2)
    assert names == {f"{WORKER_NAME}-{i}" for i in range(1, 3)}


def test_scaling_disabled(ray_cluster: RayClusterFactory) -> None:
    cluster = ray_cluster(
        scaling_enabled=False,
        num_workers=1,
    )

    with pytest.raises(ValueError, match=SCALER_NAME):
        ray.get_actor(SCALER_NAME, namespace=cluster.config.cluster_id)


def test_scale_config(ray_cluster: RayClusterFactory) -> None:
    cluster = ray_cluster(
        scaling_enabled=True,
        num_workers=1,
        min_workers=0,
        max_workers=8,
    )

    with urllib.request.urlopen(
        f"http://127.0.0.1:{cluster.config.scheduler.scaling.port}/scale_config"
    ) as response:
        body = json.loads(response.read())

    assert body["desired"] == 1
    assert body["available"] == 1
    assert body["min"] == 0
    assert body["max"] == 8


def test_client_autoscaling(
    ray_cluster: RayClusterFactory,
    run_query: TestQuery,
) -> None:
    cluster = ray_cluster(
        scaling_enabled=True,
        num_workers=0,
    )

    run_query(cluster=cluster, min_workers=2, max_workers=2)

    assert len(list_actor_names(cluster.config.cluster_id, WORKER_NAME)) == 2
