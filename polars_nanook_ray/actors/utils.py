import logging
import pathlib
import secrets
import shutil
import signal
import tempfile
import typing

import ray
from ray.actor import ActorHandle
from ray.util.state import list_actors
from ray.util.state.common import RAY_MAX_LIMIT_FROM_API_SERVER

logger = logging.getLogger(__name__)


def _resolve_config_path(instance_id: str) -> pathlib.Path:
    return pathlib.Path(
        tempfile.gettempdir(),
        f"{instance_id}-{secrets.token_hex(8)}",
        "config.toml",
    )


def _write_config(config_path: pathlib.Path, config: str) -> None:
    config_path.parent.mkdir(mode=0o700)
    config_path.write_text(config)


def _remove_config(config_path: pathlib.Path) -> None:
    shutil.rmtree(config_path.parent, ignore_errors=True)


def _handle_sigterm(func: typing.Callable[[], None]) -> None:
    previous_handler = signal.getsignal(signal.SIGTERM)

    def _handler(signum: int, frame: object) -> None:
        try:
            func()
        finally:
            if callable(previous_handler):
                previous_handler(signum, frame)  # type: ignore[arg-type]

    signal.signal(signal.SIGTERM, _handler)


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
