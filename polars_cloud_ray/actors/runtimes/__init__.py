import typing

from pydantic import BaseModel

from polars_cloud_ray.actors.runtimes.base import Runner
from polars_cloud_ray.actors.runtimes.docker import DockerRunner
from polars_cloud_ray.actors.runtimes.subprocess import SubprocessRunner
from polars_cloud_ray.config import (
    PolarsDockerConfig,
    PolarsRuntimeConfig,
    PolarsSubprocessConfig,
)

RUNNERS: dict[type[BaseModel], type[Runner]] = {
    PolarsDockerConfig: DockerRunner,
    PolarsSubprocessConfig: SubprocessRunner,
}


def resolve_runner_class(runtime: PolarsRuntimeConfig) -> type[Runner]:
    """Return the runner class registered for the `runtime` given."""
    try:
        return RUNNERS[type(runtime)]
    except KeyError:
        msg = (
            f"No runner registered for the '{type(runtime).__name__}' runtime; "
            "add one to 'polars_cloud_ray.actors.runtimes.RUNNERS'."
        )
        raise ValueError(msg) from None


def resolve_runner(
    runtime: PolarsRuntimeConfig,
    instance_id: str,
    name: str,
    **kwargs: typing.Any,
) -> Runner:
    """Return the runner of the instance given, under the `runtime` given."""
    return resolve_runner_class(runtime)(runtime, instance_id, name, **kwargs)


__all__ = [
    "RUNNERS",
    "DockerRunner",
    "Runner",
    "SubprocessRunner",
    "resolve_runner",
    "resolve_runner_class",
]
