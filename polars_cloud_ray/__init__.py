import logging
import os

from polars_cloud_ray.actors import (
    PolarsLicenseServerActor,
    PolarsScalerActor,
    PolarsSchedulerActor,
    PolarsWorkerActor,
)
from polars_cloud_ray.cluster import PolarsRayCluster
from polars_cloud_ray.config import (
    PolarsCheckpointConfig,
    PolarsEnterpriseLicenseConfig,
    PolarsLicenseConfig,
    PolarsLicenseServerConfig,
    PolarsLineageConfig,
    PolarsMonitoringConfig,
    PolarsObservatoryConfig,
    PolarsRayClusterConfig,
    PolarsRayLicenseServerConfig,
    PolarsRuntimeConfig,
    PolarsScalingConfig,
    PolarsSchedulerConfig,
    PolarsServiceAccountLicenseConfig,
    PolarsSubprocessConfig,
    PolarsWorkerConfig,
)
from polars_cloud_ray.context import RayClusterContext
from polars_cloud_ray.license_server import PolarsRayLicenseServer

__all__ = [
    "PolarsCheckpointConfig",
    "PolarsEnterpriseLicenseConfig",
    "PolarsLicenseConfig",
    "PolarsLicenseServerActor",
    "PolarsLicenseServerConfig",
    "PolarsLineageConfig",
    "PolarsMonitoringConfig",
    "PolarsObservatoryConfig",
    "PolarsRayCluster",
    "PolarsRayClusterConfig",
    "PolarsRayLicenseServer",
    "PolarsRayLicenseServerConfig",
    "PolarsRuntimeConfig",
    "PolarsScalerActor",
    "PolarsScalingConfig",
    "PolarsSchedulerActor",
    "PolarsSchedulerConfig",
    "PolarsServiceAccountLicenseConfig",
    "PolarsSubprocessConfig",
    "PolarsWorkerActor",
    "PolarsWorkerConfig",
    "RayClusterContext",
]

logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO").upper())
