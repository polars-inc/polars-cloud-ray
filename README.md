# Polars On-Prem over Ray

Runs the Polars On-Prem scheduler and workers as [Ray](https://www.ray.io/) actors.

## Quickstart

Prerequisites for a completely local test deployement:

- A Polars On-Prem binary accessible on the machine
- A Polars On-Prem `license.json` file, or a valid service account
- A Python virtual environment:

```sh
uv venv
source .venv/bin/activate
uv pip install polars-onprem-ray
```

It will also install `ray` as a dependency.
Note that a few things need to be set up for the Polars On-Prem cluster to
function properly:

```sh
# path to libpython3.x.so
export LD_LIBRARY_PATH=$(python -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))")

# need to match the config if manually changed
mkdir --parents /tmp/polars/{anonymous-results,observatory,shuffle-data,temporary-data}
```

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

If you do not already have one, create a Polars service account through the
[cloud portal](https://cloud.pola.rs/api/redirects/register).
Pull the Polars On-Prem binary locally, and remember its local path:

```sh
wget https://cdn.onprem.pola.rs/polars-on-premises-0.8.0-linux-x86
```

Spawn a local multinode cluster:

```py
import polars as pl
import polars_cloud as pc
import ray

from polars_onprem_ray.cluster import PolarsRayCluster
from polars_onprem_ray.config import (
    PolarsObservatoryConfig,
    PolarsRayClusterConfig,
    PolarsSchedulerConfig,
    PolarsServiceAccountLicenseConfig,
)

config = PolarsRayClusterConfig(
    binary_path="/path/to/binary",
    num_workers=4,
    single_host_cluster=True,
    license=PolarsServiceAccountLicenseConfig(
        client_id="<SERVICE_ACCOUNT_ID>",
        client_secret="<SERVICE_ACCOUNT_SECRET>",
    ),
    scheduler=PolarsSchedulerConfig(
        observatory=PolarsObservatoryConfig(
            database_path="/tmp/polars/observatory"
        ),
    ),
)

ray.init(address="auto", namespace=config.cluster_id)
cluster = PolarsRayCluster(config)
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
    PolarsObservatoryConfig,
    PolarsRayClusterConfig,
    PolarsSchedulerConfig,
    PolarsServiceAccountLicenseConfig,
)
from polars_onprem_ray.context import RayClusterContext

config = PolarsRayClusterConfig(
    binary_path="/path/to/binary",
    num_workers=4,
    single_host_cluster=True,
    license=PolarsServiceAccountLicenseConfig(
        client_id="<SERVICE_ACCOUNT_ID>",
        client_secret="<SERVICE_ACCOUNT_SECRET>",
    ),
    scheduler=PolarsSchedulerConfig(
        observatory=PolarsObservatoryConfig(
            database_path="/tmp/polars/observatory"
        ),
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

Enable it via `PolarsScalingConfig` on the scheduler, and optionally set
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

## License server

`PolarsOnPremLicenseServer` manages a `pc-license-server` process, Polars
On-Prem's offline license server: clusters point their configuration at it via
`PolarsOnPremLicenseServerConfig(uri=...)` and it validates them locally,
tracking usage into signed reports it periodically emits (and optionally
uploads to the control plane).

It is standalone: unlike the scheduler/worker/scaler, it is not wired into
`PolarsOnPremCluster`. It is meant to be a single, long-lived service that any
number of separate clusters register against, so its lifecycle (and Ray
namespace) is managed independently, and it should be started _before_ any
cluster that points at it.

```py
import ray

from polars_onprem_ray.config import PolarsOnPremLicenseServerRuntimeConfig
from polars_onprem_ray.license_server import PolarsOnPremLicenseServer

config = PolarsOnPremLicenseServerRuntimeConfig(
    report_dir="/var/log/polars/license-server",
    license_path="/etc/polars/license.json",
    tls_bundle_path="/etc/polars/tls-bundle.pem",
)

ray.init(address="auto", namespace="license-server")
license_server = PolarsOnPremLicenseServer(config)
license_server.start()
```

A cluster then validates against it with:

```py
from polars_onprem_ray.config import PolarsOnPremLicenseServerConfig

license = PolarsOnPremLicenseServerConfig(uri=license_server.get_bind_addr())
```

To reconnect to (or stop) an already-running license server from another
process, call `ray.init()` with the same namespace it was started under, then
`PolarsOnPremLicenseServer(config).start()` (reconnects) or `.stop()`.

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
