# Polars On-Prem over Ray

Runs the Polars On-Prem scheduler and workers as [Ray](https://www.ray.io/) actors.

## Quickstart

Prerequisites:

- A Polars On-Prem binary accessible on the machine
- A Polars On-Prem `license.json` file, or a valid service account
- A Python virtual environment:

```sh
uv venv
source .venv/bin/activate
uv pip install polars-onprem-ray
```

It will also install `ray` as a dependency.
This latter can be started using the following command:

```sh
ray start \
  --dashboard-host=0.0.0.0 \
  --disable-usage-stats \
  --head \
  --port="6379" \
  --ray-client-server-port="10001" \
  --resources='{"head":1}' # pinning
```

Note that a few things need to be set up for the Polars On-Prem cluster to
function properly:

```sh
# path to libpython3.x.so
LIBDIR=$(python -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))")
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:+${LD_LIBRARY_PATH}:}${LIBDIR}"

# need to match the config if manually changed
mkdir --parents /tmp/polars/{anonymous-results,shuffle-data,temporary-data}
```

Spawn a local multinode cluster:

```py
import polars as pl
import polars_cloud as pc
import ray

from polars_onprem_ray.cluster import PolarsOnPremCluster
from polars_onprem_ray.config import (
    PolarsOnPremClusterConfig,
    PolarsOnPremEnterpriseLicenseConfig,
    PolarsOnPremSchedulerConfig,
    PolarsOnPremWorkerConfig,
)

config = PolarsOnPremClusterConfig(
    single_host_cluster=True,
    num_workers=4,
    license=PolarsOnPremEnterpriseLicenseConfig(license_path="/path/to/license.json"),
    scheduler=PolarsOnPremSchedulerConfig(
        cpu_max=1,
        memory_max=2 * 1024**3,
        observatory=PolarsOnPremObservatoryConfig(
            database_path="/tmp/polars/observatory.db"
        ),
    ),
    worker=PolarsOnPremWorkerConfig(
        cpu_max=2,
        memory_max=4 * 1024**3,
    ),
)

ray.init(address="auto", namespace=config.cluster_id)
cluster = PolarsOnPremCluster(config)
cluster.start()

print(
    pl.LazyFrame({"a": [1, 2, 3], "b": [4, 4, 5]})
    .with_columns(pl.col("a").max().over("b").alias("c"))
    .remote(pc.ClusterContext(uri=f"http://{cluster.get_client_addr()}"))
    .execute()
    .head
)

cluster.stop()
ray.shutdown()
```

or:

```py
import polars as pl
import ray

from polars_onprem_ray.config import (
    PolarsOnPremClusterConfig,
    PolarsOnPremEnterpriseLicenseConfig,
    PolarsOnPremSchedulerConfig,
    PolarsOnPremWorkerConfig,
)
from polars_onprem_ray.context import RayClusterContext

config = PolarsOnPremClusterConfig(
    # single_host_cluster=True,
    num_workers=4,
    license=PolarsOnPremEnterpriseLicenseConfig(license_path="./license.json"),
    scheduler=PolarsOnPremSchedulerConfig(
        cpu_max=1,
        memory_max=2 * 1024**3,
        observatory=PolarsOnPremObservatoryConfig(
            database_path="/tmp/polars/observatory.db"
        ),
    ),
    worker=PolarsOnPremWorkerConfig(
        cpu_max=2,
        memory_max=4 * 1024**3,
    ),
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

In case Ray is running on a single host, set the `single_host_cluster`
configuration attribute to `True` to offsets worker ports and avoid socket
collisions.

The actors are running in `detached` mode and survive past the script: on can
reconnect with the same `ray.init()` and `cluster` gymnastics from another
process.
To clean all actors and underlying processes, Ray itself needs to be shutdown
using the following command:

```sh
ray stop --force
```

## Autoscaling

A dedicated scaler actor, pinned to the scheduler node, runs an HTTP server to
handle scaling requests sent by the underlying binary.
These requests are relayed to the scheduler actor, which in turn adds or removes
Ray worker actors in response.

Enable it via `PolarsOnPremScalingConfig` on the scheduler, and optionally set
`min_workers` and/or `max_workers` on the cluster config to bound how far it may
scale.
Requesting more workers is done via the client: `.distributed(min_workers=X)`.

> [!NOTE]
> Avoid setting `num_workers=1` if you want queries to ever trigger a scale-up.
>
> The current version of the Polars On-Prem binary plans a query as single-node
> -bypassing the autoscaler entirely- whenever its statically configured worker
> count (`n_workers`) is `1` or fewer; this configuration attribute is optional
> however, and the wrapper attribute `num_workers` exposed in this Python package
> is only sent to the binary when its value is different than `0`.

## Resource requests and limits

Four parameters control resource usage:

- `cpu_max` / `memory_max`: requested by Ray for bin-packing; not enforced at
- the OS level.
- `cpu_reserved`: a scheduling/accounting hint reported to the observatory;\
- never enforced.
- `memory_limit`: enforced by the binary itself via cgroups; but only if a
- _delegated cgroup subtree is made available_ (_e.g._, inside a container or a
- scoped `systemd-run`).

A plain session/SSH shell does not provide one (everything lives flatly in one
cgroup), so `memory_limit` fails outright with the following message:

> Ensure cgroup is mounted and subgroups are delegated, or disable the memory
> limit in the configuration file.

### What "enforced" actually means

Hitting `memory.max` does not by itself kill a process: the kernel attempts
direct reclaim and retries, and only resorts to the OOM killer once reclaim
genuinely cannot free anything more.
In practice this usually means throttling to a crawl rather than a hard
failure.
