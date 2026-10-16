import json
import subprocess
import time

import pytest
import ray
from ray.exceptions import ActorUnavailableError

from polars_cloud_ray.actors import (
    SCHEDULER_NAME_PREFIX,
    WORKER_NAME_PREFIX,
    PolarsWorkerActor,
    list_actor_names,
    resolve_worker_name,
    terminate_actors,
)

from .conftest import USE_DOCKER, RayClusterFactory, TestQuery

pytestmark = pytest.mark.skipif(
    not USE_DOCKER,
    reason="requires PYTEST_USE_DOCKER to be set",
)


def _cmd(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, **kwargs)


def _container_exists(container_id: str) -> bool:
    return _cmd(["docker", "inspect", container_id], check=False).returncode == 0


def _container_inspect(container_id: str) -> dict:
    return json.loads(_cmd(["docker", "inspect", container_id], check=True).stdout)[0]


def _container_running(name_prefix: str) -> list[str]:
    return _cmd(
        ["docker", "ps", "--filter", f"name={name_prefix}", "--format", "{{.ID}}"],
        check=True,
    ).stdout.split()


def test_as_containers(
    ray_cluster: RayClusterFactory,
    run_query: TestQuery,
) -> None:
    cluster = ray_cluster(num_workers=1)

    scheduler_container_id = _container_running(
        cluster.config.instance_name(SCHEDULER_NAME_PREFIX)
    )
    worker_container_ids = _container_running(
        cluster.config.instance_name(WORKER_NAME_PREFIX),
    )
    container_ids = scheduler_container_id + worker_container_ids

    assert all(_container_exists(container_id) for container_id in container_ids)

    result = run_query(cluster=cluster)
    assert result is not None

    cluster.stop()

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and any(
        _container_exists(container_id) for container_id in container_ids
    ):
        time.sleep(2)

    assert not any(_container_exists(container_id) for container_id in container_ids)


def test_container_running_once_started(ray_cluster: RayClusterFactory) -> None:
    cluster = ray_cluster(num_workers=0)
    scheduler_host = ray.get(cluster._scheduler_actor.get_host.remote())

    # each start races the docker daemon and loses about a third of the time, so start
    # several to catch it reliably
    workers = [
        PolarsWorkerActor.options(  # type: ignore[attr-defined]
            name=resolve_worker_name(worker_id),
            namespace=cluster.config.cluster_id,
            num_cpus=0,
            memory=cluster.config.worker.memory_hint,
        ).remote(cluster.config, worker_id, scheduler_host)
        for worker_id in range(8)
    ]

    try:
        # sent before __init__ returns, so ray runs it right after the container start;
        # a stop landing there before the container runs is a no-op it would survive
        pids = ray.get([worker.get_worker_pid.remote() for worker in workers])
        assert None not in pids
    finally:
        # ray tears down unreferenced actors itself, killing them mid docker stop
        terminate_actors(workers, cluster.config.actor_response_timeout)


def test_worker_container_reclaimed(ray_cluster: RayClusterFactory) -> None:
    cluster = ray_cluster(num_workers=1)
    worker = cluster._worker_actors[0]
    worker_name = cluster.config.instance_name(resolve_worker_name(0))

    container_ids_before = _container_running(worker_name)
    assert len(container_ids_before) == 1

    ray.kill(worker, no_restart=False)

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            if ray.get(worker.is_ready.remote()):
                break
        except ActorUnavailableError:
            pass
        time.sleep(2)

    container_ids_after = _container_running(worker_name)
    assert len(container_ids_after) == 1

    assert container_ids_after != container_ids_before


def test_resource_quotas_applied(ray_cluster: RayClusterFactory) -> None:
    cpus = 1
    memory = 500 * 1024**2

    cluster = ray_cluster(
        num_workers=1,
        worker_cpus_hint=cpus,
        worker_memory_hint=memory,
    )

    container_id = _container_running(
        cluster.config.instance_name(resolve_worker_name(0))
    )[0]
    host_config = _container_inspect(container_id)["HostConfig"]

    assert host_config["NanoCpus"] == int(cpus * 1e9)
    assert host_config["Memory"] == memory


def test_containers_scaling(ray_cluster: RayClusterFactory) -> None:
    cluster = ray_cluster(num_workers=1)
    cluster_id = cluster.config.cluster_id

    assert (
        len(_container_running(cluster.config.instance_name(WORKER_NAME_PREFIX))) == 1
    )

    cluster._rescale_worker_pool_to(2)

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        containers = _container_running(
            cluster.config.instance_name(WORKER_NAME_PREFIX)
        )
        if len(containers) == 2:
            break
        time.sleep(1)

    assert len(list_actor_names(cluster_id, WORKER_NAME_PREFIX)) == 2
    assert len(containers) == 2, "A scaled-up worker did not get a container"
