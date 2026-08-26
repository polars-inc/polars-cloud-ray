import json
import os
import pathlib
import time

import pytest
import ray

from polars_onprem_ray.actors import PolarsOnPremLicenseServerActor
from polars_onprem_ray.config import (
    PolarsOnPremLicenseServerConfig,
    PolarsOnPremLicenseServerRuntimeConfig,
)

from .conftest import RayClusterFactory, _free_port

_ENV_VARS = (
    "BINARY_PATH_LICENSE_SERVER",
    "LICENSE_PATH",
    "TLS_BUNDLE_PATH",
)

pytestmark = pytest.mark.skipif(
    not all(os.environ.get(var) for var in _ENV_VARS),
    reason=f"requires {', '.join(_ENV_VARS)} to be set",
)


def _read_reports(report_dir: pathlib.Path) -> list[dict]:
    reports = []

    for path in sorted(report_dir.glob("report.*.jsonl")):
        for line in path.read_text().splitlines():
            if not line.strip():
                continue
            try:
                reports.append(json.loads(line)["params"])
            except json.JSONDecodeError:
                continue  # report being written

    return reports


# report.tick() only happens every report_interval which is hardcoded to 5 mins in
# pc-license-server/src/main.rs; the first tick reflects an empty store, so wait
# for one that actually reflects the registered cluster
def _wait_for_report(report_dir: pathlib.Path) -> dict:
    timeout = 300 + 30  # give some room

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        reports = _read_reports(report_dir)
        if reports and reports[-1]["sequence"] > 1:  # sequence starts at 1
            return reports[-1]
        time.sleep(5)

    msg = f"No matching report in {report_dir} within {timeout}s"
    raise AssertionError(msg)


def test_license_server_reporting(
    tmp_path: pathlib.Path,
    ray_cluster: RayClusterFactory,
) -> None:
    config = PolarsOnPremLicenseServerRuntimeConfig(
        binary_path=os.environ["BINARY_PATH_LICENSE_SERVER"],
        grpc_port=_free_port(),
        http_port=_free_port(),
        report_dir=str(tmp_path),
        license_path=os.environ["LICENSE_PATH"],
        tls_bundle_path=os.environ["TLS_BUNDLE_PATH"],
    )

    license_server = PolarsOnPremLicenseServerActor.remote(config)

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if ray.get(license_server.is_ready.remote()):
            break
        time.sleep(0.5)
    else:
        msg = "License server did not become ready within 30s"
        raise AssertionError(msg)

    try:
        cluster = ray_cluster(
            license=PolarsOnPremLicenseServerConfig(
                uri=ray.get(license_server.get_bind_addr.remote())
            ),
        )

        report = _wait_for_report(tmp_path)

        assert len(report["clusters"]) == 1
        assert report["clusters"][0]["stopped_at"] is None

        cluster.stop()
    finally:
        ray.get(license_server.stop.remote())
        ray.kill(license_server)
