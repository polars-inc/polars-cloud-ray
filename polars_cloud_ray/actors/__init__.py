import typing

from pydantic import BaseModel

from polars_cloud_ray.actors.license_server import (
    LICENSE_SERVER_NAME_PREFIX,
    PolarsLicenseServerActor,
    resolve_license_server_name,
)
from polars_cloud_ray.actors.scaler import (
    SCALER_NAME_PREFIX,
    PolarsScalerActor,
    resolve_scaler_name,
)
from polars_cloud_ray.actors.scheduler import (
    SCHEDULER_NAME_PREFIX,
    PolarsSchedulerActor,
    resolve_scheduler_name,
)
from polars_cloud_ray.actors.utils import (
    list_actor_names,
    resolve_actor_handles,
    terminate_actors,
)
from polars_cloud_ray.actors.worker import (
    WORKER_NAME_PREFIX,
    PolarsWorkerActor,
    resolve_worker_name,
)
from polars_cloud_ray.config import PolarsSubprocessConfig


class RuntimeActors(typing.NamedTuple):
    license_server: typing.Any
    scaler: typing.Any
    scheduler: typing.Any
    worker: typing.Any


ACTORS: dict[type[BaseModel], RuntimeActors] = {
    PolarsSubprocessConfig: RuntimeActors(
        license_server=PolarsLicenseServerActor,
        scaler=PolarsScalerActor,
        scheduler=PolarsSchedulerActor,
        worker=PolarsWorkerActor,
    ),
}


def resolve_runtime_actors(runtime: BaseModel) -> RuntimeActors:
    """Return the actor classes running the binaries under the `runtime` given."""
    try:
        return ACTORS[type(runtime)]
    except KeyError:
        msg = (
            f"No actors registered for the '{type(runtime).__name__}' runtime; "
            "add them to 'polars_cloud_ray.actors.ACTORS'."
        )
        raise ValueError(msg) from None


__all__ = [
    "ACTORS",
    "LICENSE_SERVER_NAME_PREFIX",
    "SCALER_NAME_PREFIX",
    "SCHEDULER_NAME_PREFIX",
    "WORKER_NAME_PREFIX",
    "PolarsLicenseServerActor",
    "PolarsScalerActor",
    "PolarsSchedulerActor",
    "PolarsWorkerActor",
    "RuntimeActors",
    "list_actor_names",
    "resolve_actor_handles",
    "resolve_license_server_name",
    "resolve_runtime_actors",
    "resolve_scaler_name",
    "resolve_scheduler_name",
    "resolve_worker_name",
    "terminate_actors",
]
