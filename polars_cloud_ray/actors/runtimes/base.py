import abc
import typing


class Runner(abc.ABC):
    """Run one binary instance for an actor, in the way its runtime configures.

    These are the only spots an actor touches what it launched and how it can be
    accessed; each runtime should implement them all, and get an entry in the
    `polars_cloud_ray.actors.runtimes.RUNNERS` dict.
    """

    def __init__(self, runtime: typing.Any, instance_id: str, name: str) -> None:
        self.runtime = runtime
        self.instance_id = instance_id
        self.name = name

        self._handle: typing.Any | None = None

    @property
    def _lock(self) -> bool:
        """Return whether the binary was started, and not stopped since."""
        return self._handle is not None

    @abc.abstractmethod
    def host(self) -> str: ...

    @abc.abstractmethod
    def start(self, args: list[str], env: dict[str, str]) -> None: ...

    @abc.abstractmethod
    def stop(self) -> None: ...

    @abc.abstractmethod
    def stop_orphans(self, cluster_id: str, ports: list[int]) -> None: ...

    @abc.abstractmethod
    def is_alive(self) -> bool: ...

    @abc.abstractmethod
    def is_listening(self, port: int) -> bool: ...

    @abc.abstractmethod
    def exit_code(self) -> int | None: ...

    @abc.abstractmethod
    def pid(self) -> int | None: ...
