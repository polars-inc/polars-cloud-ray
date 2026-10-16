import functools
import logging
import pathlib
import socket
import subprocess

import psutil
import toml

from polars_cloud_ray.actors.runtimes.base import Runner
from polars_cloud_ray.config import PolarsSubprocessConfig

logger = logging.getLogger(__name__)


class SubprocessRunner(Runner):
    """Run the binary as a subprocess of the actor, as a system process on the host."""

    runtime: PolarsSubprocessConfig

    @functools.cached_property
    def host(self) -> str:
        return socket.gethostbyname(socket.gethostname())

    def start(self, args: list[str], env: dict[str, str]) -> None:
        self._handle = subprocess.Popen([self.runtime.binary_path, *args], env=env)
        logger.info("Process ID of %s: %d", self.name, self._handle.pid)

    def stop(self) -> None:
        if self._handle is not None:
            logger.info("Stopping %s (%d)", self.name, self._handle.pid)
            self._handle.terminate()
            self._wait_or_kill()
        self._handle = None

    def stop_orphans(self, cluster_id: str, ports: list[int]) -> None:
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

            if cmdline[0] != self.runtime.binary_path:
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

            if stale_config.get("instance_id") != self.instance_id:
                msg += f"by a different instance ('{stale_config.get('instance_id')}')."
                raise RuntimeError(msg)

            logger.warning(
                "Cleaning orphan '%s' process left over from a previous crash",
                self.instance_id,
            )

            process.terminate()
            try:
                process.wait(timeout=10)
            except psutil.TimeoutExpired:
                process.kill()
                process.wait()

    def is_listening(self, port: int) -> bool:
        try:
            with socket.create_connection((self.host, port), timeout=0.5):
                return True
        except OSError:
            return False

    def pid(self) -> int | None:
        return None if self._handle is None else self._handle.pid
