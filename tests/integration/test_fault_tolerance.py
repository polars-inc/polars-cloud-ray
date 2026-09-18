import contextlib
import os
import signal
import time

import psutil
import ray
from ray.actor import ActorHandle
from ray.util.state import list_actors

from polars_cloud_ray.actors import SCHEDULER_NAME_PREFIX, WORKER_NAME_PREFIX

from .conftest import RayClusterFactory, TestQuery


def _actor_pid(cluster_id: str, name: str) -> int:
    for actor in list_actors(
        filters=[
            ("ray_namespace", "=", cluster_id),
            ("state", "=", "ALIVE"),
        ]
    ):
        if actor.name == name and actor.pid is not None:
            return actor.pid

    msg = f"Actor '{name!r}' not found in namespace '{cluster_id!r}'"
    raise AssertionError(msg)


def _wait_for_worker_restart(
    cluster_id: str,
    actor: ActorHandle,
    actor_pid: int,
) -> int:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            new_actor_pid = _actor_pid(cluster_id, f"{WORKER_NAME_PREFIX}-0")
        except AssertionError:
            new_actor_pid = None

        if new_actor_pid is not None and new_actor_pid != actor_pid:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if ray.get(actor.is_ready.remote()):
                    return new_actor_pid
                time.sleep(1)

        time.sleep(1)

    msg = "New actor did not restart properly"
    raise RuntimeError(msg)


def test_worker_restart_cleanup(ray_cluster: RayClusterFactory) -> None:
    cluster = ray_cluster(num_workers=1)
    cluster_id = cluster.config.cluster_id

    actor = cluster._worker_actors[0]
    actor_pid = _actor_pid(cluster_id, f"{WORKER_NAME_PREFIX}-0")
    binary_pid = ray.get(actor.get_worker_pid.remote())

    # killing the actor will orphan the child process
    os.kill(actor_pid, signal.SIGKILL)
    time.sleep(2)

    try:
        new_actor_pid = _wait_for_worker_restart(cluster_id, actor, actor_pid)
        new_binary_pid = ray.get(actor.get_worker_pid.remote())

        assert new_actor_pid is not None
        assert new_actor_pid != actor_pid

        assert new_binary_pid != binary_pid
        assert not psutil.pid_exists(binary_pid)
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.kill(binary_pid, signal.SIGKILL)


def test_old_worker_reregister_with_new_scheduler(
    ray_cluster: RayClusterFactory,
    run_query: TestQuery,
) -> None:
    cluster = ray_cluster(num_workers=1)
    cluster_id = cluster.config.cluster_id
    scheduler_actor = cluster._scheduler_actor

    sched_pid = _actor_pid(cluster_id, SCHEDULER_NAME_PREFIX)
    binary_pid = ray.get(scheduler_actor.get_scheduler_pid.remote())

    # killing the actor will orphan the child process
    os.kill(sched_pid, signal.SIGKILL)
    with contextlib.suppress(ProcessLookupError):
        os.kill(binary_pid, signal.SIGKILL)
    time.sleep(2)

    # ray needs a moment to notice the actor died before deregistering its name; calling
    # cluster.start() before that "reconnects" to a zombie actor: still resolves but is
    # already dead
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            ray.get_actor(SCHEDULER_NAME_PREFIX, namespace=cluster_id)
        except ValueError:
            break
        time.sleep(2)
    else:
        msg = f"Ray never deregistered the killed '{SCHEDULER_NAME_PREFIX}' actor"
        raise AssertionError(msg)

    # scheduler is gone, create a fresh one (worker should still be hanging around)
    cluster.start()
    new_sched_pid = _actor_pid(cluster_id, SCHEDULER_NAME_PREFIX)
    assert new_sched_pid != sched_pid
    time.sleep(2)

    # allow some retries for reregistering
    result = None
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            result = run_query(cluster=cluster)
            break
        except Exception:
            time.sleep(2)

    assert result is not None
