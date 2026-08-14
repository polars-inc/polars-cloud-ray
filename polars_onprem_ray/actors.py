import http.server
import json
import logging
import os
import pathlib
import re
import socket
import subprocess
import tempfile
import threading

import psutil
import ray
import toml
from ray.actor import ActorHandle
from ray.util.state import list_actors

from polars_onprem_ray.config import PolarsOnPremClusterConfig

SCALER_NAME = "scaler"
SCHEDULER_NAME = "scheduler"
WORKER_NAME = "worker"

_WORKER_NAME_RE = re.compile(rf"^{WORKER_NAME}-(\d+)$")

logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO").upper())
logger = logging.getLogger(__name__)


def _resolve_host() -> str:
    return socket.gethostbyname(socket.gethostname())


def _stop(process: subprocess.Popen | None, label: str) -> None:
    if process is not None:
        logger.info("Stopping %s (%d)", label, process.pid)
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def _stop_orphans(
    binary_path: str,
    cluster_id: str,
    instance_id: str,
    ports: list[int],
) -> None:
    """Terminate a leftover process from a previous crashed instance."""
    for process in psutil.process_iter():
        try:
            port = next(
                (
                    conn.laddr.port
                    for conn in process.net_connections(kind="tcp")
                    if conn.status == psutil.CONN_LISTEN and conn.laddr.port in ports
                ),
                None,
            )
            if port is None:
                continue
            cmdline = process.cmdline()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

        msg = f"Port {port} is already in use by process ID {process.pid} "

        if cmdline[0] != binary_path:
            msg += "which does not look like one of our own processes."
            raise RuntimeError(msg)

        try:
            stale_config = toml.loads(pathlib.Path(cmdline[3]).read_text())
        except OSError as exc:
            msg += "but we cannot confirm if it is a stale instance."
            raise RuntimeError(msg) from exc

        if stale_config.get("cluster_id") != cluster_id:
            msg += f"by a different cluster ('{stale_config.get('cluster_id')}')."
            raise RuntimeError(msg)

        if stale_config.get("instance_id") != instance_id:
            msg += f"by a different instance ('{stale_config.get('instance_id')}')."
            raise RuntimeError(msg)

        logger.warning(
            "Cleaning orphan '%s' process left over from a previous crash",
            instance_id,
        )

        process.terminate()
        try:
            process.wait(timeout=10)
        except psutil.TimeoutExpired:
            process.kill()
            process.wait()


def _is_ready(
    process: subprocess.Popen | None,
    host: str,
    port: int,
) -> bool:
    if process is None or process.poll() is not None:
        return False
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def list_actor_names(cluster_id: str, prefix: str) -> set[str]:
    return {
        actor.name
        for actor in list_actors(
            address=ray.get_runtime_context().gcs_address,
            filters=[
                ("ray_namespace", "=", cluster_id),
                ("state", "=", "ALIVE"),
            ],
        )
        if isinstance(actor.name, str) and actor.name.startswith(prefix)
    }


@ray.remote
class PolarsOnPremSchedulerActor:
    """The central service, distributing tasks and piloting autoscaling."""

    def __init__(self, config: PolarsOnPremClusterConfig) -> None:
        self.config: PolarsOnPremClusterConfig = config

        self.scheduler_host: str = _resolve_host()
        self.num_workers: int = config.num_workers  # initial value

        self._config_path: str | None = None
        self._process: subprocess.Popen | None = None

        self.start()

    def _add_worker(self, worker_id: int) -> None:
        actor_name = f"{WORKER_NAME}-{worker_id}"

        PolarsOnPremWorkerActor.options(  # type: ignore[attr-defined]
            name=actor_name,
            namespace=self.config.cluster_id,
            lifetime="detached",
            max_restarts=self.config.worker_max_restarts,
            num_cpus=self.config.worker.cpu_max,
            memory=self.config.worker.memory_max,
        ).remote(self.config, worker_id, self.scheduler_host)

        logger.info("Requested new worker %s", actor_name)

    def _remove_worker(self, actor_name: str) -> None:
        try:
            actor = ray.get_actor(actor_name, namespace=self.config.cluster_id)
        except ValueError:
            logger.warning("Worker %s not found, ignoring remove request", actor_name)
            return

        ray.get(actor.stop.remote())
        ray.kill(actor)

        logger.info("Removed worker %s", actor_name)

    def start(self) -> None:
        """Spawn the scheduler process, if not already running."""
        if self._process is not None:
            return

        _stop_orphans(
            self.config.binary_path,
            self.config.cluster_id,
            "scheduler",
            [
                self.config.scheduler.worker_registration_port,
                self.config.scheduler.client_port,
            ],
        )

        toml = self.config.config_scheduler(self.scheduler_host)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml)
            self._config_path = f.name

        logger.info(
            "Starting scheduler (%s observatory) on %s",
            "and" if self.config.scheduler.observatory.enabled else "without",
            self.scheduler_host,
        )

        cmd = [self.config.binary_path, "service", "--config-path", self._config_path]
        env = {
            **os.environ,
            "POLARS_EULA_ACCEPTED": "yes" if self.config.accept_eula else "no",
            "PLC_LOG_LEVEL": self.config.log_level,
            "RUST_BACKTRACE": self.config.rust_backtrace,
        }

        self._process = subprocess.Popen(cmd, env=env)
        logger.info("Scheduler process ID: %d", self._process.pid)

    def stop(self) -> None:
        """Terminate the scheduler process and clean up."""
        _stop(self._process, "scheduler")
        self._process = None

        if self._config_path is not None:
            pathlib.Path(self._config_path).unlink()
            self._config_path = None

    def is_ready(self) -> bool:
        """Return whether the scheduler process is alive and accepting registration."""
        return _is_ready(
            self._process,
            self.scheduler_host,
            self.config.scheduler.worker_registration_port,
        )

    def get_host(self) -> str:
        """Return the IP address the scheduler is running on."""
        return self.scheduler_host

    def get_client_addr(self) -> str:
        """Return the URI queries should be submitted to."""
        return f"{self.scheduler_host}:{self.config.scheduler.client_port}"

    def get_dashboard_addr(self) -> str | None:
        """Return the observatory dashboard URL."""
        return (
            f"http://{self.scheduler_host}:{self.config.scheduler.observatory.rest_port}"
            if self.config.scheduler.observatory.enabled
            else None
        )

    def get_scheduler_pid(self) -> int | None:
        """Return the OS process ID of the scheduler binary subprocess."""
        return self._process.pid if self._process is not None else None

    def get_scaling_status(self) -> dict[str, int | set[str]]:
        """Return the current/desired worker counts and configured bounds."""
        return {
            "available": list_actor_names(self.config.cluster_id, WORKER_NAME),
            "desired": self.num_workers,
            "min": self.config.min_workers,
            # unbounded scaling is set to u32::MAX in the rust code, which
            # translates to the following hex value; this value is non-optional
            "max": self.config.max_workers or 0xFFFFFFFF,
        }

    def rescale_worker_pool_to(
        self,
        num_workers: int,
        delete: set[str] | None = None,
        keep: set[str] | None = None,
    ) -> None:
        """Rescale the number of workers according to the scheduler requests.

        Parameters
        ----------
        num_workers
            The number of worker to upscale or downscale to.
        delete
            Set of worker instances to terminate.
        keep
            Set of worker instances to keep running, while terminating all the others.

        """
        worker_names = list_actor_names(self.config.cluster_id, WORKER_NAME)
        self.num_workers = num_workers  # new value

        if len(worker_names) < num_workers:
            # offset the id used by each worker to avoid collisions with running worker
            # names
            offset = (
                max(
                    [
                        int(m.group(1))
                        for actor_name in worker_names
                        if (m := _WORKER_NAME_RE.match(actor_name)) is not None
                    ],
                    default=-1,
                )
                + 1
            )
            for worker_id in range(num_workers - len(worker_names)):
                self._add_worker(worker_id + offset)

        if len(worker_names) > num_workers:
            # actor.name is always set to config.worker.instance_id
            if num_workers == 0:
                for instance_id in worker_names:
                    self._remove_worker(instance_id)
            else:
                if delete is not None:
                    for instance_id in delete:
                        self._remove_worker(instance_id)
                if keep is not None:
                    for instance_id in worker_names - keep:
                        self._remove_worker(instance_id)


@ray.remote
class PolarsOnPremWorkerActor:
    """The computing service, accepting tasks from the scheduler."""

    def __init__(
        self,
        config: PolarsOnPremClusterConfig,
        worker_id: int,
        scheduler_host: str,
    ) -> None:
        self.config: PolarsOnPremClusterConfig = config

        self.worker_id = worker_id
        self.worker_host: str = _resolve_host()
        self.scheduler_host = scheduler_host

        self._config_path: str | None = None
        self._process: subprocess.Popen | None = None

        self.start()

    def start(self) -> None:
        """Spawn the worker process, if not already running."""
        if self._process is not None:
            return

        offset = self.config._worker_port_offset(self.worker_id)

        _stop_orphans(
            self.config.binary_path,
            self.config.cluster_id,
            f"worker-{self.worker_id}",
            [
                self.config.worker.task_port + offset,
                self.config.worker.shuffle_port + offset,
            ],
        )

        toml = self.config.config_worker(
            self.worker_id,
            self.worker_host,
            self.scheduler_host,
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(toml)
            self._config_path = f.name

        logger.info("Starting worker-%d on %s", self.worker_id, self.worker_host)

        cmd = [self.config.binary_path, "service", "--config-path", self._config_path]
        env = {
            **os.environ,
            "POLARS_EULA_ACCEPTED": "yes" if self.config.accept_eula else "no",
            "POLARS_TEMP_DIR": self.config.worker.temporary_data_dir,
            "PLC_LOG_LEVEL": self.config.log_level,
            "RUST_BACKTRACE": self.config.rust_backtrace,
        }

        self._process = subprocess.Popen(cmd, env=env)
        logger.info("PID of %s-%d: %d", WORKER_NAME, self.worker_id, self._process.pid)

    def stop(self) -> None:
        """Terminate the worker process and clean up."""
        _stop(self._process, f"worker-{self.worker_id}")
        self._process = None

        if self._config_path is not None:
            pathlib.Path(self._config_path).unlink()
            self._config_path = None

    def is_ready(self) -> bool:
        """Return whether the worker process is alive and listening for tasks."""
        port = self.config.worker.task_port + self.config._worker_port_offset(
            self.worker_id
        )
        return _is_ready(self._process, self.worker_host, port)

    def get_worker_pid(self) -> int | None:
        """Return the OS process ID of the worker binary subprocess."""
        return self._process.pid if self._process is not None else None


class _ScalingRequestHandler(http.server.BaseHTTPRequestHandler):
    """Bridge HTTP scaling calls from the binary to the scheduler actor."""

    server: "_ScalingHTTPServer"

    # resolve lazily on each call rather than once at construction: the scaler
    # should start before the scheduler actor for the latter to bind to it
    def _scheduler_actor(self) -> ActorHandle:
        return ray.get_actor(SCHEDULER_NAME, namespace=self.server.cluster_id)

    def _respond(self, status: int, payload: dict | None = None) -> None:
        body = json.dumps(payload).encode() if payload is not None else b""

        self.send_response(status)

        if body:
            self.send_header("Content-Type", "application/json")

        self.send_header("Content-Length", str(len(body)))
        self.end_headers()

        if body:
            self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path != "/scale_config":
            self._respond(404, {"detail": "not found"})
            return

        try:
            status = ray.get(self._scheduler_actor().get_scaling_status.remote())
        except Exception as exc:
            logger.exception("scale_config request failed")
            self._respond(500, {"detail": str(exc)})
            return

        self._respond(
            200,
            {
                "available": len(status["available"]),
                "desired": status["desired"],
                "min": status["min"],
                "max": status["max"],
            },
        )

    def do_POST(self) -> None:
        if self.path != "/scale_to":
            self._respond(404, {"detail": "not found"})
            return

        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}
        keep = set(body.get("workers_to_keep") or []) or None
        delete = set(body.get("workers_to_delete") or []) or None
        logger.info(
            "scale_to request: amount=%d, keep=%s, delete=%s",
            body["amount"],
            keep,
            delete,
        )

        try:
            ray.get(
                self._scheduler_actor().rescale_worker_pool_to.remote(
                    body["amount"],
                    delete=delete,
                    keep=keep,
                )
            )
        except Exception as exc:
            logger.exception("scale_to request failed")
            self._respond(500, {"detail": str(exc)})
            return

        self._respond(204)

    def log_message(self, format: str, *args: object) -> None:
        logger.debug(format, *args)


class _ScalingHTTPServer(http.server.ThreadingHTTPServer):
    """A `ThreadingHTTPServer` that carries the cluster ID to its handlers."""

    def __init__(self, address: tuple[str, int], cluster_id: str) -> None:
        self.cluster_id = cluster_id
        super().__init__(address, _ScalingRequestHandler)


@ray.remote
class PolarsOnPremScalerActor:
    """Bridge service relaying scaling requests to the scheduler actor."""

    def __init__(self, config: PolarsOnPremClusterConfig) -> None:
        self.config: PolarsOnPremClusterConfig = config

        self.http_server: _ScalingHTTPServer | None = None
        self._thread: threading.Thread | None = None

        self.start()

    def start(self) -> None:
        """Start the HTTP server, if not already running."""
        if self.http_server is None:
            # fails with OSError if port is already used
            self.http_server = _ScalingHTTPServer(
                ("127.0.0.1", self.config.scheduler.scaling.port),
                self.config.cluster_id,
            )
            self._thread = threading.Thread(
                target=self.http_server.serve_forever,
                daemon=True,
            )
            self._thread.start()

            logger.info(
                "Scaler HTTP server listening on 127.0.0.1:%d",
                self.http_server.server_port,
            )

    def stop(self) -> None:
        """Stop the HTTP server, if running."""
        if self.http_server is not None:
            self.http_server.shutdown()
            self.http_server.server_close()
            self.http_server = None
            self._thread = None

    def is_ready(self) -> bool:
        """Return whether the HTTP server is listening."""
        return self.http_server is not None
