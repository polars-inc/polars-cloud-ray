import ray
from polars.testing import assert_frame_equal
from polars_onprem_ray.actors import SCHEDULER_NAME_PREFIX, WORKER_NAME_PREFIX, list_actor_names
from polars_onprem_ray.cluster import PolarsOnPremCluster

from .conftest import RayClusterConfigFactory, TestQuery


def test_reconnect_existing_cluster(
    ray_cluster_config: RayClusterConfigFactory,
    run_query: TestQuery,
) -> None:
    config = ray_cluster_config(num_workers=1)

    ray.init(address="auto", namespace=config.cluster_id, ignore_reinit_error=True)
    cluster0 = PolarsOnPremCluster(config)
    cluster0.start()

    scheduler0 = ray.get_actor(SCHEDULER_NAME_PREFIX, namespace=config.cluster_id)
    workers0 = [
        ray.get_actor(actor_name, namespace=config.cluster_id)
        for actor_name in list_actor_names(config.cluster_id, WORKER_NAME_PREFIX)
    ]
    result0 = run_query(cluster=cluster0)

    ray.shutdown()  # simulates the driver process exiting (cluster stays up)

    ray.init(address="auto", namespace=config.cluster_id, ignore_reinit_error=True)
    cluster1 = PolarsOnPremCluster(config)
    cluster1.start()

    scheduler1 = ray.get_actor(SCHEDULER_NAME_PREFIX, namespace=config.cluster_id)
    workers1 = [
        ray.get_actor(actor_name, namespace=config.cluster_id)
        for actor_name in list_actor_names(config.cluster_id, WORKER_NAME_PREFIX)
    ]
    result1 = run_query(cluster=cluster1)

    try:
        assert scheduler1 == scheduler0
        assert workers1 == workers0

        assert result0 is not None
        assert result1 is not None
        assert_frame_equal(result0, result1, check_row_order=False)

        cluster1.stop()
    finally:
        ray.shutdown()
