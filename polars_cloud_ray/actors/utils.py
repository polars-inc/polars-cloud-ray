import logging
import os
import pathlib
import signal
import socket
import subprocess
import typing

import psutil
import ray
import toml
from ray.actor import ActorHandle
from ray.util.state import list_actors
from ray.util.state.common import RAY_MAX_LIMIT_FROM_API_SERVER

logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO").upper())
logger = logging.getLogger(__name__)


def _resolve_host() -> str:
    return socket.gethostbyname(socket.gethostname())


def _stop(process: subprocess.Popen | None, label: str) -> None:
    if process is not None:
        logger.info("Stopping %s (%d)", label, process.pid)
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def _stop_orphans(
    binary_path: str,
    cluster_id: str,
    instance_id: str,
    ports: list[int],
) -> None:
    """Terminate a leftover process from a previous crashed instance."""
    for process in psutil.process_iter():
        try:
            port = next(
                (
                    conn.laddr.port
                    for conn in process.net_connections(kind="tcp")
                    if conn.status == psutil.CONN_LISTEN and conn.laddr.port in ports
                ),
                None,
            )
            if port is None:
                continue
            cmdline = process.cmdline()
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
            continue

        msg = f"Port {port} is already in use by process ID {process.pid} "

        if cmdline[0] != binary_path:
            msg += "which does not look like one of our own processes."
            raise RuntimeError(msg)

        try:
            stale_config = toml.loads(pathlib.Path(cmdline[3]).read_text())
        except (IndexError, OSError) as exc:
            msg += "but we cannot confirm if it is a stale instance."
            raise RuntimeError(msg) from exc

        if stale_config.get("cluster_id") != cluster_id:
            msg += f"by a different cluster ('{stale_config.get('cluster_id')}')."
            raise RuntimeError(msg)

        if stale_config.get("instance_id") != instance_id:
            msg += f"by a different instance ('{stale_config.get('instance_id')}')."
            raise RuntimeError(msg)

        logger.warning(
            "Cleaning orphan '%s' process left over from a previous crash",
            instance_id,
        )

        process.terminate()
        try:
            process.wait(timeout=10)
        except psutil.TimeoutExpired:
            process.kill()
            process.wait()


def _handle_sigterm(func: typing.Callable[[], None]) -> None:
    """Call function on `SIGTERM` call; then hand off to Ray's handler."""
    previous_handler = signal.getsignal(signal.SIGTERM)

    def _handler(signum: int, frame: object) -> None:
        try:
            func()
        finally:
            if callable(previous_handler):
                previous_handler(signum, frame)  # type: ignore[arg-type]

    signal.signal(signal.SIGTERM, _handler)


def _is_listening(host: str, port: int) -> bool:
    """Return whether something is accepting connections on `host:port`."""
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


class SubprocessRunMixin:
    """Run the binary as a subprocess of the actor.

    The four methods below are the only spots an actor touches what it launched, so an
    alternative managing context (a container, or a `systemd-run` scope for instance)
    needs to overwrite them.

    These methods are inherited, and the whole process lifecycle around them remains
    untouched. `_handle` is whatever that context needs to identify what it started.
    """

    _handle: typing.Any | None = None

    def _start(self, cmd: list[str], env: dict[str, str], name: str) -> None:
        self._handle = subprocess.Popen(cmd, env=env)
        logger.info("Process ID of %s: %d", name, self._handle.pid)

    def _stop(self, name: str) -> None:
        _stop(self._handle, name)
        self._handle = None

    def _is_alive(self) -> bool:
        return self._handle is not None and self._handle.poll() is None

    def _pid(self) -> int | None:
        return self._handle.pid if self._handle is not None else None


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
