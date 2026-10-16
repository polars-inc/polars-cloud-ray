import abc
import subprocess
import typing

from polars_cloud_ray.config import PolarsRuntimeConfig


class Runner(abc.ABC):
    """Run one binary instance for an actor, in the way its runtime configures.

    These are the only spots an actor touches what it launched and how it can be
    accessed; each runtime should implement them all, and get an entry in the
    `polars_cloud_ray.actors.runtimes.RUNNERS` dict.
    """

    def __init__(
        self,
        runtime: PolarsRuntimeConfig,
        instance_id: str,
        name: str,
        **kwargs: typing.Any,  # noqa: ARG002
    ) -> None:
        self.runtime = runtime
        self.instance_id = instance_id
        self.name = name

        self._handle: subprocess.Popen[bytes] | None = None

    def _exit_code(self) -> int | None:
        """Return the binary exit code, negative for a signal, or `None` until then."""
        return None if self._handle is None else self._handle.poll()

    def _raise_if_exited(self) -> None:
        """Raise if the binary exited, as nothing restarts it."""
        exit_code = self._exit_code()

        if exit_code is None:
            return

        reason = (
            f"was killed by signal {-exit_code}"
            if exit_code < 0
            else f"exited with code {exit_code}"
        )
        msg = f"{self.instance_id} {reason}, see the actor logs for its output"
        raise RuntimeError(msg)

    def _wait_or_kill(self, timeout: float = 10) -> None:
        """Wait for the binary to exit after a stop request, killing it on timeout."""
        if self._handle is None:
            return

        try:
            self._handle.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self._handle.kill()
            self._handle.wait()

    @property
    @abc.abstractmethod
    def host(self) -> str:
        """Return the address the scheduler and workers reach the binary on."""

    @abc.abstractmethod
    def start(self, args: list[str], env: dict[str, str]) -> None:
        """Launch the binary with the arguments given, without waiting for it."""

    @abc.abstractmethod
    def stop(self) -> None:
        """Stop the binary, killing it if needed; a no-op when not started."""

    @abc.abstractmethod
    def stop_orphans(self, cluster_id: str, ports: list[int]) -> None:
        """Reclaim what a crashed previous instance left, before starting anew."""

    def is_alive(self) -> bool:
        """Return whether the binary is running right now."""
        return self._handle is not None and self._handle.poll() is None

    @abc.abstractmethod
    def is_listening(self, port: int) -> bool:
        """Return whether the binary accepts connections on the port given."""

    def is_started(self) -> bool:
        """Return whether the binary was started, and not stopped since."""
        return self._handle is not None

    @abc.abstractmethod
    def pid(self) -> int | None:
        """Return the process ID of the binary on the host, if it is running."""
