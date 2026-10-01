import logging
import signal
import typing

import ray
from ray.actor import ActorHandle
from ray.util.state import list_actors
from ray.util.state.common import RAY_MAX_LIMIT_FROM_API_SERVER

logger = logging.getLogger(__name__)


def _handle_sigterm(func: typing.Callable[[], None]) -> None:
    previous_handler = signal.getsignal(signal.SIGTERM)

    def _handler(signum: int, frame: object) -> None:
        try:
            func()
        finally:
            if callable(previous_handler):
                previous_handler(signum, frame)  # type: ignore[arg-type]

    signal.signal(signal.SIGTERM, _handler)


def _raise_if_exited(name: str, exit_code: int | None) -> None:
    # nothing restarts the underlying binary, so waiting for it is pointless
    if exit_code is None:
        return

    reason = (
        f"was killed by signal {-exit_code}"
        if exit_code < 0
        else f"exited with code {exit_code}"
    )
    msg = f"{name} {reason}, see the actor logs for its output"
    raise RuntimeError(msg)


def list_actor_names(cluster_id: str, prefix: str) -> set[str]:
    return {
        actor.name
        for actor in list_actors(
            address=ray.get_runtime_context().gcs_address,
            filters=[
                ("ray_namespace", "=", cluster_id),
                ("state", "!=", "DEAD"),
            ],
            limit=RAY_MAX_LIMIT_FROM_API_SERVER,
        )
        if isinstance(actor.name, str) and actor.name.startswith(prefix)
    }


def resolve_actor_handles(names: set[str], namespace: str) -> dict[str, ActorHandle]:
    actors: dict[str, ActorHandle] = {}

    for name in names:
        try:
            actors[name] = ray.get_actor(name, namespace=namespace)
        except ValueError:
            logger.warning("Actor %s not found, skipping", name)

    return actors


def terminate_actors(actors: list[ActorHandle], timeout: float) -> None:
    if len(actors):
        try:
            ray.get([actor.stop.remote() for actor in actors], timeout=timeout)
        except Exception:
            logger.exception("One or more actors failed to stop cleanly")
        finally:
            for actor in actors:
                ray.kill(actor)  # no-op if the actor is already gone
