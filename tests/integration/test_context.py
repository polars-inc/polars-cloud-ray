import ray

from polars_cloud_ray.context import RayClusterContext

from .conftest import RayClusterConfigFactory, TestQuery


def test_ray_cluster_context(
    ray_cluster_config: RayClusterConfigFactory,
    run_query: TestQuery,
) -> None:
    ray.init(address="auto", ignore_reinit_error=True)
    try:
        with RayClusterContext(ray_cluster_config(num_workers=1)) as ctx:
            result = run_query(context=ctx)
    finally:
        ray.shutdown()

    assert result is not None
