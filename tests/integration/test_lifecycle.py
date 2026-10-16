import pathlib
import time

import pytest
import ray
from polars.testing import assert_frame_equal

from polars_cloud_ray.cluster import PolarsRayCluster
from polars_cloud_ray.config import PolarsEnterpriseLicenseConfig

from .conftest import RayClusterConfigFactory, RayClusterFactory, TestQuery


def test_two_sequential_polars_clusters_one_ray_cluster(
    ray_cluster_config: RayClusterConfigFactory,
    run_query: TestQuery,
) -> None:
    ray.init(address="auto", ignore_reinit_error=True)
    try:
        cluster0 = PolarsRayCluster(ray_cluster_config(num_workers=1))
        cluster0.start()
        result0 = run_query(cluster=cluster0)
        cluster0.stop()

        cluster1 = PolarsRayCluster(ray_cluster_config(num_workers=1))
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
        cluster0 = PolarsRayCluster(ray_cluster_config(num_workers=1))
        cluster0.start()

        cluster1 = PolarsRayCluster(ray_cluster_config(num_workers=1))
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


def test_scheduler_exit_fails_fast(
    ray_cluster: RayClusterFactory,
    tmp_path: pathlib.Path,
) -> None:
    license = PolarsEnterpriseLicenseConfig(license_path=str(tmp_path / "missing.json"))

    started = time.monotonic()
    with pytest.raises(RuntimeError, match="scheduler exited with code"):
        ray_cluster(num_workers=0, license=license)

    assert time.monotonic() - started < 15
