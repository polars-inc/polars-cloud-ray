import pathlib
import socket
import typing

import toml
from pydantic import BaseModel, ConfigDict, Field, model_validator


def _resolve_absolute_path(path: str) -> pathlib.Path:
    return pathlib.Path(path).expanduser().resolve()


def _port_available(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((host, port))
        except OSError:
            return False
        return True


class AbsLocationConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    storage_type: typing.Literal["abs"] = "abs"

    url: str = Field(
        description=(
            "The entire Azure Blob Storage URI. If the storage location requires "
            "authentication, provide the credentials via `options`."
        ),
    )
    options: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Storage options for the Azure Blob Storage location. These correspond to "
            "Object Store's `AzureConfigKey`. More info: "
            "https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html"
        ),
    )
    presign_duration: str | None = Field(
        default=None,
        description=(
            "The duration for which presigned URLs remain valid. Either an ISO 8601 "
            "duration format or a jiff friendly duration format (see "
            "https://docs.rs/jiff/0.2.18/jiff/fmt/friendly/), e.g. `5 secs` or `PT5S`."
        ),
    )
    allow_deletes: bool | None = Field(
        default=None,
        description="Whether the object-store client may issue delete requests.",
    )

    def config(self) -> dict:
        section = self.model_dump(
            exclude={"storage_type", "options"}, exclude_none=True
        )
        return {self.storage_type: section | self.options}


class GcsLocationConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    storage_type: typing.Literal["gcs"] = "gcs"

    url: str = Field(
        description=(
            "The entire Google Cloud Storage URI. If this storage location requires "
            "authentication, provide the credentials via `options`."
        ),
    )
    options: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Storage options for the Google Cloud Storage location. These correspond "
            "to Object Store's `GoogleConfigKey`. More info: "
            "https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html"
        ),
    )
    presign_duration: str | None = Field(
        default=None,
        description=(
            "The duration for which presigned URLs remain valid. Either an ISO 8601 "
            "duration format or a jiff friendly duration format (see "
            "https://docs.rs/jiff/0.2.18/jiff/fmt/friendly/), e.g. `5 secs` or `PT5S`."
        ),
    )
    allow_deletes: bool | None = Field(
        default=None,
        description="Whether the object-store client may issue delete requests.",
    )

    def config(self) -> dict:
        section = self.model_dump(
            exclude={"storage_type", "options"}, exclude_none=True
        )
        return {self.storage_type: section | self.options}


class S3LocationConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    storage_type: typing.Literal["s3"] = "s3"

    url: str = Field(
        description=(
            "The entire S3 URI, e.g. `s3://bucket/prefix`. If the storage location "
            "requires authentication, provide the credentials via `options`."
        ),
    )
    options: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Storage options for the AWS S3 storage location. These correspond to "
            "Object Store's `AmazonS3ConfigKey`. More info: "
            "https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html"
        ),
    )
    external_endpoint: str | None = Field(
        default=None,
        description="Override endpoint URL for S3-compatible stores (e.g. MinIO).",
    )
    presign_duration: str | None = Field(
        default=None,
        description=(
            "The duration for which presigned URLs remain valid. Either an ISO 8601 "
            "duration format or a jiff friendly duration format (see "
            "https://docs.rs/jiff/0.2.18/jiff/fmt/friendly/), e.g. `5 secs` or `PT5S`."
        ),
    )
    allow_deletes: bool | None = Field(
        default=None,
        description="Whether the object-store client may issue delete requests.",
    )

    def config(self) -> dict:
        section = self.model_dump(
            exclude={"storage_type", "options"}, exclude_none=True
        )
        return {self.storage_type: section | self.options}


class LocalStorageConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    storage_type: typing.Literal["local"] = "local"

    path: str = Field(description="Absolute path on the local filesystem.")

    def config(self) -> dict:
        return {self.storage_type: self.model_dump(exclude={"storage_type"})}


class SharedFilesystemStorageConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    storage_type: typing.Literal["shared_filesystem"] = "shared_filesystem"

    path: str = Field(
        description=(
            "The filesystem path shared across all cluster nodes (e.g. an NFS or CSI "
            "driver mount). Must be reachable at the same path on every node."
        ),
    )

    def config(self) -> dict:
        return {self.storage_type: self.model_dump(exclude={"storage_type"})}


StorageLocation = typing.Annotated[
    AbsLocationConfig
    | GcsLocationConfig
    | S3LocationConfig
    | LocalStorageConfig
    | SharedFilesystemStorageConfig,
    Field(discriminator="storage_type"),
]


class LineageHttpConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    endpoint: str = Field(description="HTTP endpoint lineage events are sent to.")


class LineageStdioConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    output_stream: str = Field(
        default="stdout",
        description=(
            'Standard stream lineage events are written to ("stdout" or "stderr").'
        ),
    )


class PolarsOnPremLineageConfig(BaseModel):
    """Enable support for lineage exporting to a specific endpoint."""

    model_config = ConfigDict(frozen=True)

    http: LineageHttpConfig | None = Field(
        default=None,
        description="Emit lineage events over HTTP.",
    )
    stdio: LineageStdioConfig | None = Field(
        default=None,
        description="Emit lineage events to a standard stream.",
    )

    def config(self) -> dict | None:
        d: dict = {}

        if self.http:
            d["http"] = {"endpoint": self.http.endpoint}
        if self.stdio:
            d["stdio"] = {"output_stream": self.stdio.output_stream}

        return {"transport": d} if d else None


class PolarsOnPremCheckpointConfig(BaseModel):
    """Checkpointing for queries.

    Checkpointing is enabled automatically whenever a checkpoint location is
    configured; the scheduler then periodically checkpoints completed stages so
    queries can resume after failures.
    """

    model_config = ConfigDict(frozen=True)

    period: str = Field(
        description=(
            "Period at which checkpoints will be created. If the period has passed "
            "after a stage has completed, a checkpoint will be created. Accepts either "
            "a jiff friendly duration (e.g. `5s`, see "
            "https://docs.rs/jiff/latest/jiff/fmt/friendly/) or an ISO 8601 duration "
            "(e.g. `PT5S`)."
        ),
    )


class PolarsOnPremEnterpriseLicenseConfig(BaseModel):
    """Validate the license from a local offline license file."""

    model_config = ConfigDict(frozen=True)
    license_type: typing.Literal["on_prem_enterprise"] = "on_prem_enterprise"

    license_path: str = Field(
        description=(
            "Absolute path to the file containing your Polars license key "
            '("license.json").'
        ),
    )

    def config(self) -> dict:
        return {self.license_type: self.model_dump(exclude={"license_type"})}


class PolarsOnPremLicenseServerConfig(BaseModel):
    """Validate the license against a Polars license server."""

    model_config = ConfigDict(frozen=True)
    license_type: typing.Literal["license_server"] = "license_server"

    uri: str = Field(description="License server URI.")

    def config(self) -> dict:
        return {self.license_type: self.model_dump(exclude={"license_type"})}


class PolarsOnPremServiceAccountLicenseConfig(BaseModel):
    """Validate the license online via the Polars control plane."""

    model_config = ConfigDict(frozen=True)
    license_type: typing.Literal["on_prem"] = "on_prem"

    client_id: str = Field(description="Control-plane OAuth client ID.")
    client_secret: str = Field(description="Control-plane OAuth client secret.")
    workspace_id: str = Field(description="Control-plane workspace ID.")
    cert_dir: str = Field(
        default="/etc/polars_license_cert",
        description=(
            "Directory containing TLS certificates for control-plane communication."
        ),
    )

    def config(self) -> dict:
        return {
            self.license_type: self.model_dump(
                exclude={"license_type"}, exclude_none=True
            )
        }


PolarsOnPremLicenseConfig = typing.Annotated[
    PolarsOnPremEnterpriseLicenseConfig
    | PolarsOnPremServiceAccountLicenseConfig
    | PolarsOnPremLicenseServerConfig,
    Field(discriminator="license_type"),
]


class PolarsOnPremTlsConnectionConfig(BaseModel):
    """Require TLS for a service."""

    model_config = ConfigDict(frozen=True)

    certificate_key_file: str = Field(
        description="Absolute path to the PEM certificate/key file.",
    )
    private_key_file: str = Field(
        description="Absolute path to the PEM private key file.",
    )

    def config(self) -> dict:
        return {"tls": self.model_dump()}


class PolarsOnPremJwksAuthConfig(BaseModel):
    """Require a JWT validated against a JWKS endpoint for a service."""

    model_config = ConfigDict(frozen=True)

    url: str = Field(description="JWKS endpoint URL.")
    issuers: str = Field(description="Accepted JWT issuer.")
    audience: str = Field(description="Accepted JWT audience.")

    def config(self) -> dict:
        return {"jwks": self.model_dump()}


class PolarsOnPremObservatoryConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool = Field(
        default=True,
        description="Enable the observatory module.",
    )
    cluster_mode: str = Field(
        default="bare_metal",
        description="Observatory cluster mode label.",
    )
    max_metrics_bytes_total: int = Field(
        default=104857600,
        description="Maximum number of bytes for host metrics storage.",
    )
    database_export: str | None = Field(
        default=None,
        description="Path metrics are periodically exported to, if set.",
    )
    database_path: str = Field(
        default="/var/log/polars/observatory.db",
        description=(
            "Path to the observatory SQLite database. Its parent directory must exist."
        ),
    )
    otlp_port: int = Field(
        default=5049,
        description="Port for the OpenTelemetry trace/metrics receiver.",
    )
    rest_port: int = Field(
        default=3001,
        description="Port for the observatory REST API and dashboard.",
    )
    rest_connection: PolarsOnPremTlsConnectionConfig | None = Field(
        default=None,
        description="Require TLS on the REST API/dashboard service. Disabled if unset.",
    )
    rest_auth: PolarsOnPremJwksAuthConfig | None = Field(
        default=None,
        description=(
            "Require a JWT validated against a JWKS endpoint on the REST "
            "API/dashboard service. Disabled if unset."
        ),
    )

    def config(self) -> dict:
        d: dict = {"enabled": self.enabled}

        if self.enabled:
            rest_service: dict = {"bind_addr": f":{self.rest_port}"}
            if self.rest_connection is not None:
                rest_service["connection"] = self.rest_connection.config()
            if self.rest_auth is not None:
                rest_service["auth"] = self.rest_auth.config()

            d |= {
                "max_metrics_bytes_total": self.max_metrics_bytes_total,
                "cluster_mode": self.cluster_mode,
                "database_path": self.database_path,
                "service": {"bind_addr": f":{self.otlp_port}"},
                "rest_api": {"service": rest_service},
            }

            if self.database_export:
                d["database_export"] = self.database_export

        return d


class PolarsOnPremMonitoringConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool = Field(
        default=True,
        description="Enable the monitoring module.",
    )
    disable_host_metrics: bool = Field(
        default=False,
        description="Disable host metrics collection for the dashboard.",
    )

    def config(self) -> dict:
        d: dict = {"enabled": self.enabled}

        if self.enabled:
            d["host_metrics"] = {"enabled": not self.disable_host_metrics}

        return d


class PolarsOnPremScalingConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool = Field(
        default=False,
        description=(
            "Enable REST-based autoscaling module. When enabled, the scheduler actor "
            "runs a local HTTP server that ou binary pushes scale requests to."
        ),
    )
    port: int = Field(
        default=4001,
        description=("Port the autoscaler HTTP server listens for requests on."),
    )

    def config(self) -> dict | None:
        # next two lines for older binaries without support for the [scaling] section
        # will be removed later on
        if not self.enabled:
            return None

        d: dict = {"enabled": self.enabled}

        if self.enabled:
            d["rest"] = {"uri": f"http://127.0.0.1:{self.port}"}

        return d


class PolarsOnPremSchedulerConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    # cluster configuration
    cpu_max: int = Field(
        description="Number of CPU cores requested for the scheduler actor.",
    )
    memory_max: int = Field(
        description="Max memory, in bytes, requested for the scheduler actor.",
    )

    # native binary configuration
    cpu_reserved: float | None = Field(
        default=None,
        description=(
            "Number of CPU cores reserved for our binary execution. Not enforced via "
            "cgroups; only a scheduling/accounting hint consumed internally for task "
            "placement and reported to the observatory."
        ),
    )
    memory_limit: int | None = Field(
        default=None,
        description=(
            "Memory limit, in bytes, enforced by our binary via cgroups. Requires a "
            "delegated cgroup subtree, which only container runtimes provide."
        ),
    )

    client_port: int = Field(
        default=5051,
        description=(
            "Port for the scheduler's client gRPC service, used for query submission."
        ),
    )
    worker_registration_port: int = Field(
        default=5050,
        description="Port workers register with the scheduler on.",
    )
    connection: PolarsOnPremTlsConnectionConfig | None = Field(
        default=None,
        description="Require TLS on the client-facing gRPC service. Disabled if unset.",
    )
    auth: PolarsOnPremJwksAuthConfig | None = Field(
        default=None,
        description=(
            "Require a JWT validated against a JWKS endpoint on the client-facing "
            "gRPC service. Disabled if unset."
        ),
    )
    deny_anonymous_users: bool = Field(
        default=False,
        description=(
            "Enabling this option ensures that all queries must be sent with a set "
            "username."
        ),
    )
    anonymous_result: StorageLocation | None = Field(
        default_factory=lambda: LocalStorageConfig(
            path="/tmp/polars/anonymous-results",
        ),
        description=(
            "Ephemeral storage for queries that don't specify a result location. "
            "Recommended to use S3 for persistence of results. The compute plane does "
            "not automatically clean up anonymous results."
        ),
    )
    allow_local_scans: bool = Field(
        default=True,
        description=(
            "Disabling this option prevents the worker from reading from local disk. "
            "It is currently not possible to configure which scan locations are "
            "allowed. Users can alternatively configure scans that read from S3. "
            "More info: "
            "https://docs.pola.rs/user-guide/io/cloud-storage/#reading-from-cloud-storage"
        ),
    )
    allow_local_sinks: bool = Field(
        default=True,
        description=(
            "Disabling this option prevents the worker from writing to local disk. It "
            "is currently not possible to configure which sink locations are allowed. "
            "Users can alternatively configure sinks that write to S3. More info: "
            "https://docs.pola.rs/user-guide/io/cloud-storage/#writing-to-cloud-storage"
        ),
    )
    default_partitions_per_worker: int | None = Field(
        default=None,
        description=(
            "Default number of partitions assigned per worker, if not specified by the "
            "query."
        ),
    )
    checkpoint: PolarsOnPremCheckpointConfig | None = Field(
        default=None,
        description="Scheduler checkpointing configuration. Disabled if unset.",
    )
    observatory: PolarsOnPremObservatoryConfig = Field(
        default_factory=PolarsOnPremObservatoryConfig,
        description="Observatory (metrics, tracing, dashboard) configuration.",
    )
    scaling: PolarsOnPremScalingConfig = Field(
        default_factory=PolarsOnPremScalingConfig,
        description="Autoscaling (ScalingService REST callback) configuration.",
    )

    def config_static_leader(self, scheduler_host: str) -> dict:
        return {
            "leader_instance_id": "scheduler",
            "scheduler_service": {
                "public_addr": f"{scheduler_host}:{self.worker_registration_port}"
            },
            "observatory_service": {
                "public_addr": f"{scheduler_host}:{self.observatory.otlp_port}"
            },
        }

    def config_license(self, license_: PolarsOnPremLicenseConfig) -> dict:
        return license_.config()

    def config(self, num_workers: int) -> dict:
        client_service: dict = {"bind_addr": f":{self.client_port}"}
        if self.connection is not None:
            client_service["connection"] = self.connection.config()
        if self.auth is not None:
            client_service["auth"] = self.auth.config()

        d: dict = {
            "enabled": True,
            "allow_local_scans": self.allow_local_scans,
            "allow_local_sinks": self.allow_local_sinks,
            "deny_anonymous_users": self.deny_anonymous_users,
            "client_service": client_service,
            "worker_service": {"bind_addr": f":{self.worker_registration_port}"},
        }

        if num_workers > 0:
            d["n_workers"] = num_workers
        if self.default_partitions_per_worker is not None:
            d["default_partitions_per_worker"] = self.default_partitions_per_worker
        if self.anonymous_result is not None:
            d["anonymous_result_location"] = self.anonymous_result.config()
        if self.checkpoint is not None:
            d["checkpoint"] = {"period": self.checkpoint.period}

        return d


class PolarsOnPremWorkerConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    # cluster configuration
    cpu_max: int | None = Field(
        default=None,
        description="Number of CPU cores requested for each worker actor.",
    )
    memory_max: int = Field(
        description="Max memory, in bytes, requested for each worker actor.",
    )

    # native binary configuration
    cpu_reserved: float | None = Field(
        default=None,
        description=(
            "Number of CPU cores reserved for our binary execution. Not enforced via "
            "cgroups; only a scheduling/accounting hint consumed internally for task "
            "placement and reported to the observatory."
        ),
    )
    memory_limit: int | None = Field(
        default=None,
        description=(
            "Memory limit, in bytes, enforced by our binary via cgroups. Requires a "
            "delegated cgroup subtree, which only container runtimes provide."
        ),
    )

    task_port: int = Field(
        default=5052,
        description=(
            "Port for the worker's task dispatch service (scheduler => worker)."
        ),
    )
    shuffle_port: int = Field(
        default=5053,
        description=(
            "Port for the worker's shuffle data transfer service (worker <=> worker)."
        ),
    )
    heartbeat_period_secs: int = Field(
        default=5,
        description=(
            "Heartbeat interval between polars workers and the scheduler, in seconds."
        ),
    )
    shuffle_data: StorageLocation | None = Field(
        default_factory=lambda: LocalStorageConfig(path="/tmp/polars/shuffle-data"),
        description="Ephemeral storage for shuffle data used during query execution.",
    )
    checkpoint_location: StorageLocation | None = Field(
        default=None,
        description=(
            "Storage location for checkpoint data. Checkpointing is enabled "
            "automatically once this is configured (see `scheduler.checkpoint`)."
        ),
    )
    env_vars: dict[str, str] = Field(
        default_factory=dict,
        description="Additional environment variables passed to the worker process.",
    )
    temporary_data_dir: str = Field(
        default="/tmp/polars/temporary-data",
        description=(
            "Directory for temporary data used in polars (e.g. streaming spill data). "
            "Recommended to use fast local storage for best performance. Sets "
            "POLARS_TEMP_DIR."
        ),
    )

    def config_license(self, license_: PolarsOnPremLicenseConfig) -> dict | None:
        if isinstance(license_, PolarsOnPremServiceAccountLicenseConfig):
            return None
        return license_.config()

    def config(self, task_port: int, shuffle_port: int, worker_host: str) -> dict:
        d: dict = {
            "enabled": True,
            "task_service": {
                "bind_addr": f":{task_port}",
                "public_addr": f"{worker_host}:{task_port}",
            },
            "shuffle_service": {
                "bind_addr": f":{shuffle_port}",
                "public_addr": f"{worker_host}:{shuffle_port}",
            },
            "heartbeat_period": f"{self.heartbeat_period_secs}s",
        }

        if self.shuffle_data is not None:
            d["shuffle_location"] = self.shuffle_data.config()
        if self.checkpoint_location is not None:
            d["checkpoint_location"] = self.checkpoint_location.config()
        if self.env_vars:
            d["env_vars"] = self.env_vars

        return d


class PolarsOnPremClusterConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    # cluster configuration
    binary_path: str = Field(
        default="polars-on-premises",
        description="Path to the binary.",
    )
    single_host_cluster: bool = Field(
        default=False,
        description=(
            "Offset each worker's ports by worker_id so multiple workers can run on "
            "the same host without port collisions."
        ),
    )
    worker_max_restarts: int = Field(
        default=3,
        description="Ray actor restart limit for workers (-1 = unlimited).",
    )
    worker_startup_timeout: int = Field(
        default=10,
        description="Seconds to wait for the scheduler and workers to become ready.",
    )
    actor_response_timeout: int = Field(
        default=30,
        description="Seconds to wait for actor calls to complete before giving up.",
    )

    # native binary configuration
    cluster_id: str = Field(
        default="polars-onprem",
        description=(
            "Unique identifier for the Polars cluster, used to identify it in a "
            "multi-tenant environment. Also used as the Ray actor namespace."
        ),
    )
    num_workers: int = Field(
        default=0,
        description="Number of Polars worker replicas to start.",
    )
    min_workers: int = Field(
        default=0,
        description=(
            "Minimum number of worker replicas the scheduler may scale down to."
        ),
    )
    max_workers: int | None = Field(
        default=None,
        description=(
            "Maximum number of worker replicas the scheduler may scale up to. "
            "Unbounded if unset."
        ),
    )
    accept_eula: bool = Field(
        default=False,
        description=(
            "To run our binary with an On-Prem Enterprise license, you must accept the "
            "EULA. If you don't accept it, our binary refuses to start and prints the "
            "EULA instead. Sets POLARS_EULA_ACCEPTED."
        ),
    )
    license: PolarsOnPremLicenseConfig = Field(
        description=(
            "License configuration: online control-plane, offline enterprise file, "
            "or a license server."
        ),
    )
    scheduler: PolarsOnPremSchedulerConfig = Field(
        description="Scheduler actor configuration.",
    )
    worker: PolarsOnPremWorkerConfig = Field(
        description="Worker actor configuration, shared by all workers.",
    )
    monitoring: PolarsOnPremMonitoringConfig = Field(
        default_factory=PolarsOnPremMonitoringConfig,
        description="Monitoring (host metrics) configuration.",
    )
    lineage: PolarsOnPremLineageConfig | None = Field(
        default=None,
        description="Lineage event transport configuration. Disabled if unset.",
    )
    log_level: str = Field(
        default="info",
        description='One of "info", "debug", "trace". Sets PLC_LOG_LEVEL.',
    )
    rust_backtrace: str = Field(
        default="full",
        description=(
            'Controls Rust panic backtraces. One of "0" (disabled), "1" (enabled), or '
            '"full" (enabled with extra detail, e.g. source paths for dependencies). '
            "Sets RUST_BACKTRACE."
        ),
    )

    @model_validator(mode="after")
    def _absolute_binary_path(self) -> "PolarsOnPremClusterConfig":
        """Resolve `binary_path` to an absolute path."""
        object.__setattr__(
            self, "binary_path", str(_resolve_absolute_path(self.binary_path))
        )
        return self

    def _worker_port_offset(self, worker_id: int) -> int:
        """Per-worker port offset to avoid collisions when co-located on one host."""
        return worker_id * 2 if self.single_host_cluster else 0

    def _port_availability(
        self,
        *,
        check_scheduler: bool,
        check_workers: typing.Collection[int],
        check_scaler: bool,
    ) -> None:
        """Raise if needed ports are already taken (only meaningful on single-host)."""
        if not self.single_host_cluster:
            return

        ports: dict[str, int] = {}

        if check_scheduler:
            ports["scheduler.client_port"] = self.scheduler.client_port
            ports["scheduler.worker_registration_port"] = (
                self.scheduler.worker_registration_port
            )
            if self.scheduler.observatory.enabled:
                ports["scheduler.observatory.otlp_port"] = (
                    self.scheduler.observatory.otlp_port
                )
                ports["scheduler.observatory.rest_port"] = (
                    self.scheduler.observatory.rest_port
                )

        for worker_id in check_workers:
            offset = self._worker_port_offset(worker_id)
            ports[f"worker-{worker_id}.task_port"] = self.worker.task_port + offset
            ports[f"worker-{worker_id}.shuffle_port"] = (
                self.worker.shuffle_port + offset
            )

        if check_scaler and self.scheduler.scaling.enabled:
            ports["scheduler.scaling.port"] = self.scheduler.scaling.port

        taken = [
            f"{name} ({port})"
            for name, port in ports.items()
            if not _port_available("127.0.0.1", port)
        ]

        if taken:
            msg = f"Port(s) already in use, aborting: {', '.join(taken)}"
            raise RuntimeError(msg)

    def config_scheduler(self, scheduler_host: str) -> str:
        """Render the TOML configuration of the scheduler node."""
        doc: dict = {
            "instance_id": "scheduler",
            "cluster_id": self.cluster_id,
            "memory_limit": self.scheduler.memory_limit,
            "cpu_reserved": (
                None
                if self.scheduler.cpu_reserved is None
                else str(self.scheduler.cpu_reserved)
            ),
            "license": self.scheduler.config_license(self.license),
            "static_leader": self.scheduler.config_static_leader(scheduler_host),
            "scheduler": self.scheduler.config(self.num_workers),
            "observatory": self.scheduler.observatory.config(),
            "monitoring": self.monitoring.config(),
            "scaling": self.scheduler.scaling.config(),
        }

        if self.lineage is not None and (lineage := self.lineage.config()):
            doc["lineage"] = lineage

        return toml.dumps(doc)

    def config_worker(
        self,
        worker_id: int,
        worker_host: str,
        scheduler_host: str,
    ) -> str:
        """Render the TOML configuration of worker nodes."""
        offset = self._worker_port_offset(worker_id)
        task_port = self.worker.task_port + offset
        shuffle_port = self.worker.shuffle_port + offset

        doc: dict = {
            "instance_id": f"worker-{worker_id}",
            "cluster_id": self.cluster_id,
            "memory_limit": self.worker.memory_limit,
            "cpu_reserved": (
                None
                if self.worker.cpu_reserved is None
                else str(self.worker.cpu_reserved)
            ),
            "static_leader": self.scheduler.config_static_leader(scheduler_host),
            "worker": self.worker.config(task_port, shuffle_port, worker_host),
            "monitoring": self.monitoring.config(),
        }

        if (license_ := self.worker.config_license(self.license)) is not None:
            doc["license"] = license_
        if self.lineage is not None and (lineage := self.lineage.config()):
            doc["lineage"] = lineage

        return toml.dumps(doc)
