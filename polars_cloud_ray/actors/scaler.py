import http.server
import json
import logging
import threading

import ray
from ray.actor import ActorHandle

from polars_cloud_ray.actors.scheduler import resolve_scheduler_name
from polars_cloud_ray.actors.utils import _handle_sigterm
from polars_cloud_ray.config import PolarsRayClusterConfig

SCALER_NAME_PREFIX = "scaler"

logger = logging.getLogger(__name__)


def resolve_scaler_name() -> str:
    return SCALER_NAME_PREFIX


class _ScalingRequestHandler(http.server.BaseHTTPRequestHandler):
    """Bridge HTTP scaling calls from the binary to the scheduler actor."""

    server: "_ScalingHTTPServer"

    # resolve lazily on each call rather than once at construction: the scaler
    # should start before the scheduler actor for the latter to bind to it
    def _scheduler_actor(self) -> ActorHandle:
        return ray.get_actor(
            resolve_scheduler_name(),
            namespace=self.server.cluster_id,
        )

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
            status = ray.get(  # type:ignore[var-annotated]
                self._scheduler_actor().get_scaling_status.remote(),
                timeout=self.server.actor_response_timeout,
            )
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

        try:
            body = json.loads(self.rfile.read(length)) if length else {}
        except json.JSONDecodeError:
            logger.exception("scale_to request failed")
            self._respond(400, {"detail": "malformed JSON request body"})
            return

        if "amount" not in body:
            logger.error("scale_to request failed")
            self._respond(500, {"detail": "missing 'amount' attribute in request body"})
            return

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
                ),
                timeout=self.server.actor_response_timeout,
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

    def __init__(
        self,
        address: tuple[str, int],
        cluster_id: str,
        actor_response_timeout: int,
    ) -> None:
        self.cluster_id = cluster_id
        self.actor_response_timeout = actor_response_timeout
        super().__init__(address, _ScalingRequestHandler)


@ray.remote
class PolarsScalerActor:
    """Bridge service relaying scaling requests to the scheduler actor."""

    def __init__(self, config: PolarsRayClusterConfig) -> None:
        self.config: PolarsRayClusterConfig = config

        self._http_server: _ScalingHTTPServer | None = None
        self._http_server_thread: threading.Thread | None = None

        self.start()
        _handle_sigterm(self.stop)

    def __ray_shutdown__(self) -> None:
        self.stop()

    def start(self) -> None:
        """Start the HTTP server, if not already running."""
        if self._http_server is None:
            # fails with OSError if port is already used
            self._http_server = _ScalingHTTPServer(
                ("127.0.0.1", self.config.scheduler.scaling.port),
                self.config.cluster_id,
                self.config.actor_response_timeout,
            )
            self._http_server_thread = threading.Thread(
                target=self._http_server.serve_forever,
                daemon=True,
            )
            self._http_server_thread.start()

            logger.info(
                "Scaler HTTP server listening on 127.0.0.1:%d",
                self._http_server.server_port,
            )

    def stop(self) -> None:
        """Stop the HTTP server, if running."""
        if self._http_server is not None:
            self._http_server.shutdown()
            self._http_server.server_close()
            self._http_server = None
            self._http_server_thread = None

    def is_ready(self) -> bool:
        """Return whether the HTTP server is listening."""
        return self._http_server is not None
