import logging
import pathlib
import socket
import subprocess
import typing

import psutil
import toml

logger = logging.getLogger(__name__)


class SubprocessRunMixin:
    """Run the binary as a subprocess of the actor.

    The eight methods below are the only spots an actor touches what it launched and
    how it can be accessed, so an alternative managing context (a container, or a
    `systemd-run` scope for instance) needs to overwrite them.

    These methods are inherited, and the whole process lifecycle around them remains
    untouched. `_handle` is whatever that context needs to identify what it started.
    """

    _handle: typing.Any | None = None

    def _host(self, instance_id: str) -> str:  # noqa: ARG002
        return socket.gethostbyname(socket.gethostname())

    def _start(self, args: list[str], env: dict[str, str], name: str) -> None:
        self._handle = subprocess.Popen(
            [self._runtime.binary_path, *args],  # type: ignore[attr-defined]
            env=env,
        )
        logger.info("Process ID of %s: %d", name, self._handle.pid)

    def _stop(self, name: str) -> None:
        if self._handle is not None:
            logger.info("Stopping %s (%d)", name, self._handle.pid)
            self._handle.terminate()
            try:
                self._handle.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._handle.kill()
                self._handle.wait()
        self._handle = None

    def _stop_orphans(self, instance_id: str, ports: list[int]) -> None:
        for process in psutil.process_iter():
            try:
                port = next(
                    (
                        conn.laddr.port
                        for conn in process.net_connections(kind="tcp")
                        if conn.status == psutil.CONN_LISTEN
                        and conn.laddr.port in ports
                    ),
                    None,
                )
                if port is None:
                    continue
                cmdline = process.cmdline()
            except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
                continue

            msg = f"Port {port} is already in use by process ID {process.pid} "

            if cmdline[0] != self._runtime.binary_path:  # type: ignore[attr-defined]
                msg += "which does not look like one of our own processes."
                raise RuntimeError(msg)

            try:
                stale_config = toml.loads(pathlib.Path(cmdline[3]).read_text())
            except (IndexError, OSError) as exc:
                msg += "but we cannot confirm if it is a stale instance."
                raise RuntimeError(msg) from exc

            if stale_config.get("cluster_id") != self.config_cluster.cluster_id:  # type: ignore[attr-defined]
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

    def _is_alive(self) -> bool:
        return self._handle is not None and self._handle.poll() is None

    def _is_listening(self, instance_id: str, port: int) -> bool:
        try:
            with socket.create_connection((self._host(instance_id), port), timeout=0.5):
                return True
        except OSError:
            return False

    def _exit_code(self) -> int | None:
        return self._handle.poll() if self._handle is not None else None

    def _pid(self) -> int | None:
        return self._handle.pid if self._handle is not None else None
