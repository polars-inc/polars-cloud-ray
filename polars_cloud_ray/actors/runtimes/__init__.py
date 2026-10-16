import typing

from pydantic import BaseModel

from polars_cloud_ray.actors.runtimes.base import Runner
from polars_cloud_ray.actors.runtimes.subprocess import SubprocessRunner
from polars_cloud_ray.config import PolarsRuntimeConfig, PolarsSubprocessConfig

RUNNERS: dict[type[BaseModel], type[Runner]] = {
    PolarsSubprocessConfig: SubprocessRunner,
}


def resolve_runner(
    runtime: PolarsRuntimeConfig,
    instance_id: str,
    name: str,
    **kwargs: typing.Any,
) -> Runner:
    """Return the runner of the instance given, under the `runtime` given."""
    try:
        runner_cls = RUNNERS[type(runtime)]
    except KeyError:
        msg = (
            f"No runner registered for the '{type(runtime).__name__}' runtime; "
            "add one to 'polars_cloud_ray.actors.runtimes.RUNNERS'."
        )
        raise ValueError(msg) from None

    return runner_cls(runtime, instance_id, name, **kwargs)


__all__ = [
    "RUNNERS",
    "Runner",
    "SubprocessRunner",
    "resolve_runner",
]
