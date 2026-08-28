import ray
from polars.testing import assert_frame_equal

from polars_onprem_ray.cluster import PolarsOnPremCluster

from .conftest import RayClusterConfigFactory, TestQuery


def test_two_sequential_polars_clusters_one_ray_cluster(
    ray_cluster_config: RayClusterConfigFactory,
    run_query: TestQuery,
) -> None:
    ray.init(address="auto", ignore_reinit_error=True)
    try:
        cluster0 = PolarsOnPremCluster(ray_cluster_config(num_workers=1))
        cluster0.start()
        result0 = run_query(cluster=cluster0)
        cluster0.stop()

        cluster1 = PolarsOnPremCluster(ray_cluster_config(num_workers=1))
        cluster1.start()
        result1 = run_query(cluster=cluster1)
        cluster1.stop()

        assert result0 is not None
        assert result1 is not None
        assert_frame_equal(result0, result1, check_row_order=False)
    finally:
        ray.shutdown()


def test_two_parallel_polars_clusters_one_ray_cluster(
    ray_cluster_config: RayClusterConfigFactory,
    run_query: TestQuery,
) -> None:
    ray.init(address="auto", ignore_reinit_error=True)
    try:
        cluster0 = PolarsOnPremCluster(ray_cluster_config(num_workers=1))
        cluster0.start()

        cluster1 = PolarsOnPremCluster(ray_cluster_config(num_workers=1))
        cluster1.start()

        result0 = run_query(cluster=cluster0)
        result1 = run_query(cluster=cluster1)

        assert result0 is not None
        assert result1 is not None
        assert_frame_equal(result0, result1, check_row_order=False)

        cluster0.stop()
        cluster1.stop()
    finally:
        ray.shutdown()
