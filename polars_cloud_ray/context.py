from polars_cloud import ClientOptions, ClusterContext

from polars_cloud_ray.cluster import PolarsRayCluster


class RayClusterContext(PolarsRayCluster, ClusterContext):
    """A `PolarsRayCluster` directly usable as a `polars_cloud.ClusterContext`.

    ```py
    import polars as pl
    import ray

    from polars_cloud_ray.config import (
        PolarsObservatoryConfig,
        PolarsRayClusterConfig,
        PolarsSchedulerConfig,
        PolarsServiceAccountLicenseConfig,
        PolarsWorkerConfig,
    )
    from polars_cloud_ray.context import RayClusterContext

    config = PolarsRayClusterConfig(
        # single_host_cluster=True,
        num_workers=4,
        license=PolarsServiceAccountLicenseConfig(
            workspace_id="<WORKSPACE_ID>",
            client_id="<SERVICE_ACCOUNT_ID>",
            client_secret="<SERVICE_ACCOUNT_SECRET>",
        ),
        scheduler=PolarsSchedulerConfig(
            observatory=PolarsObservatoryConfig(
                database_path="/tmp/polars/observatory/observatory.db"
            ),
        ),
        worker=PolarsWorkerConfig(),
    )

    ray.init(address="auto", namespace=config.cluster_id)

    with RayClusterContext(config) as ctx:
        print(
            pl.LazyFrame({"a": [1, 2, 3], "b": [4, 4, 5]})
            .with_columns(pl.col("a").max().over("b").alias("c"))
            .remote(ctx)
            .execute()
            .head
        )

    ray.shutdown()
    ```
    """

    def start(self) -> None:
        """Start (or reconnect to) the Ray actors, then bootstrap the client."""
        super().start()

        bootstrap = ClusterContext(
            uri=f"http://{self.get_client_addr()}",
            observatory=(
                ClientOptions(uri=self.get_dashboard_addr())
                if self.config.scheduler.observatory.enabled
                else None
            ),
        )

        self._direct_client = bootstrap._direct_client
        self._compute_id = bootstrap._compute_id
        self._connection_mode = bootstrap._connection_mode

    def __enter__(self) -> "RayClusterContext":
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()
