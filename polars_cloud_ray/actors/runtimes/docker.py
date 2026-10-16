import errno
import functools
import ipaddress
import logging
import os
import pathlib
import socket
import subprocess
import time

from polars_cloud_ray.actors.runtimes.base import Runner
from polars_cloud_ray.config import PolarsDockerConfig, _resolve_absolute_path

logger = logging.getLogger(__name__)


def _filter_mount_paths(paths: set[pathlib.Path]) -> list[pathlib.Path]:
    mounts: list[pathlib.Path] = []

    # drop any path already mounted by a shallower mount
    for path in sorted(paths, key=lambda p: (len(p.parts), p)):
        if not any(path == mount or path.is_relative_to(mount) for mount in mounts):
            mounts.append(path)

    return mounts


def _inspect_container(name: str, template: str) -> str:
    return subprocess.run(
        ["docker", "inspect", "--format", template, name],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()


def _inspect_network_gateways(network: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "docker",
            "network",
            "inspect",
            "--format",
            "{{range .IPAM.Config}}{{.Gateway}} {{end}}",
            network,
        ],
        capture_output=True,
        text=True,
        check=False,
    )


def _resolve_network_gateway(network: str) -> str | None:
    result = _inspect_network_gateways(network)

    if result.returncode != 0:
        subprocess.run(
            ["docker", "network", "create", network],
            capture_output=True,
            check=False,
        )
        result = _inspect_network_gateways(network)
        result.check_returncode()

    gateways = result.stdout.split()

    gateway = next(
        (ip for ip in gateways if ipaddress.ip_address(ip).version == 4),
        None,
    )
    if gateway is None:
        msg = f"Docker network {network} has no IPv4 gateway"
        raise RuntimeError(msg)

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((gateway, 0))
    except OSError as exc:
        if exc.errno != errno.EADDRNOTAVAIL:
            raise
        # daemon runs in a vm and delivers host.docker.internal traffic to the loopback
        return None

    return gateway


class DockerRunner(Runner):
    """Run the binary in a Docker container, through the `docker` CLI."""

    runtime: PolarsDockerConfig

    def __init__(
        self,
        runtime: PolarsDockerConfig,
        instance_id: str,
        name: str,
        *,
        cpus: float | None = None,
        memory: int | None = None,
        mounts_rw: set[pathlib.Path] | None = None,
        mounts_ro: set[pathlib.Path] | None = None,
        ports: set[int] | None = None,
    ) -> None:
        super().__init__(runtime, instance_id, name)

        # extra attributes required by this runtime
        self.cpus = cpus
        self.memory = memory
        self.mounts_rw = mounts_rw or set()
        self.mounts_ro = mounts_ro or set()
        self.ports = ports or set()

    @classmethod
    def gateway_addr(cls, runtime: PolarsDockerConfig) -> str:
        return _resolve_network_gateway(runtime.network) or "127.0.0.1"

    @functools.cached_property
    def host(self) -> str:
        return self.name

    def start(self, args: list[str], env: dict[str, str]) -> None:
        gateway = _resolve_network_gateway(self.runtime.network)

        args_env = (
            f"--env={key}={value}"
            for key, value in env.items()
            # exclude host-related environment that could clash
            if key not in ("LD_LIBRARY_PATH", "PATH", "PYTHONPATH", "VIRTUAL_ENV")
        )

        args_publish = (f"--publish=127.0.0.1:{port}:{port}" for port in self.ports)

        args_resource: list[str] = []
        if self.cpus is not None and self.cpus > 0:
            args_resource.append(f"--cpus={self.cpus}")
        if self.memory is not None and self.memory > 0:
            args_resource.append(f"--memory={int(self.memory / 1024**2)}m")

        mounts_rw = _filter_mount_paths(
            {_resolve_absolute_path(path) for path in self.runtime.extra_volume_mounts}
            | self.mounts_rw
        )
        if len(mounts_rw):
            logger.info(
                "Will mount the following paths as read-write volumes: %s",
                sorted(str(path) for path in mounts_rw),
            )
            for path in mounts_rw:
                if not path.exists():
                    path.mkdir(parents=True)
        mounts_ro = _filter_mount_paths(
            {path for path in self.mounts_ro if path.exists()}
        )
        if len(mounts_ro):
            logger.info(
                "Will mount the following paths as read-only volumes: %s",
                sorted(str(path) for path in mounts_ro),
            )
        args_volumes = [
            *(f"--volume={path}:{path}" for path in mounts_rw),
            *(f"--volume={path}:{path}:ro" for path in mounts_ro),
        ]

        logger.info("Starting %s container", self.name)
        subprocess.run(
            [
                "docker",
                "create",
                f"--name={self.name}",
                f"--network={self.runtime.network}",
                # on linux, host-gateway is the default bridge's gateway, not this one's
                f"--add-host=host.docker.internal:{gateway or 'host-gateway'}",
                "--rm",
                f"--user={os.getuid()}:{os.getgid()}",
                f"--workdir={pathlib.Path.cwd()}",
                *args_env,
                *args_publish,
                *args_resource,
                *args_volumes,
                self.runtime.image,
                *args,
            ],
            env=env,
            check=True,
        )

        # attaching relays the output to the actor logs ray collects, and attaching
        # before starting keeps the output of a binary failing at boot; the attached
        # client starts the container asynchronously, hence the while loop below
        self._handle = subprocess.Popen(
            ["docker", "start", "--attach", self.name],
            env=env,
        )
        while (
            self._handle.poll() is None
            and _inspect_container(self.name, "{{.State.Status}}") == "created"
        ):
            time.sleep(0.1)

        logger.info("Started %s container", self.name)

    def stop(self) -> None:
        if self._handle is None:
            return

        logger.info("Stopping %s container", self.name)
        subprocess.run(
            ["docker", "stop", self.name],
            capture_output=True,
            check=False,
        )
        self._wait_or_kill()

        self._handle = None
        logger.info("Stopped %s container", self.name)

    def stop_orphans(self, cluster_id: str, ports: list[int]) -> None:  # noqa: ARG002
        logger.info("Reclaiming any leftover %s container", self.name)
        subprocess.run(
            ["docker", "rm", "--force", self.name],
            capture_output=True,
            check=False,
        )

    def is_listening(self, port: int) -> bool:
        # probe from within the container as a published port is immediately bound, and
        # probing from the host would always succeed
        return (
            subprocess.run(
                [
                    "docker",
                    "exec",
                    self.name,
                    "bash",
                    "-c",
                    f"exec 3<>/dev/tcp/127.0.0.1/{port}",
                ],
                capture_output=True,
                check=False,
            ).returncode
            == 0
        )

    def pid(self) -> int | None:
        return (
            None
            if self._handle is None
            # docker reports 0 when the container is not running
            else int(_inspect_container(self.name, "{{.State.Pid}}") or 0) or None
        )
