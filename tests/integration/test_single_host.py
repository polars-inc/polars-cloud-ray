import socket

import pytest
import ray

from .conftest import USE_DOCKER, RayClusterFactory

pytestmark = pytest.mark.skipif(
    USE_DOCKER,
    reason="containers get no port offsets",
)


def _is_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


def test_port_offset(ray_cluster: RayClusterFactory) -> None:
    cluster = ray_cluster(num_workers=2)
    host = ray.get(cluster._scheduler_actor.get_host.remote())
    port = cluster.config.worker.task_port

    assert _is_open(host, port)
    assert _is_open(host, port + 2)
