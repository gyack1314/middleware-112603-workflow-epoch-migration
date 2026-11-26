"""基于标准库 http.server 的项目接口适配层。

协调引擎本身与传输无关，这里只做 JSON over HTTP 的薄封装，供不同版本的
执行端领取/回执、值班员查询。所有错误映射为 4xx，响应体保持统一结构。
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .engine import Coordinator
from .errors import ConflictError, NotFoundError, TransferBlockedError, ValidationError

ROUTES = {
    ("POST", "/definitions"): "publish_definition",
    ("POST", "/compat"): "publish_compat",
    ("POST", "/instances"): "start",
    ("POST", "/signals"): "signal",
    ("POST", "/claims"): "claim",
    ("POST", "/receipts"): "settle",
    ("POST", "/transfers"): "request_migration",
    ("POST", "/transfers/abort"): "abort_transfer",
    ("POST", "/restart"): "restart",
    ("GET", "/instances"): "inspect",
}

# 字段名从 HTTP 习惯翻译到引擎关键字参数。
FIELD_ALIASES = {
    "publish_definition": {"version": "version", "steps": "steps"},
    "publish_compat": {"from": "from_version", "to": "to_version", "steps": "mapping"},
    "start": {"instance": "instance", "version": "version"},
    "signal": {
        "instance": "instance", "version": "version", "step": "step",
        "id": "signal_id", "reason": "reason",
    },
    "claim": {
        "instance": "instance", "version": "version", "step": "step",
        "attempt": "attempt",
    },
    "settle": {"attempt": "attempt", "state": "state"},
    "request_migration": {
        "instance": "instance", "request": "request_id", "to": "to_version",
    },
    "abort_transfer": {"instance": "instance", "request": "request_id"},
    "restart": {},
    "inspect": {"instance": "instance"},
}

ERROR_STATUS = (
    (ValidationError, 400),
    (NotFoundError, 404),
    (ConflictError, 409),
    (TransferBlockedError, 423),  # Locked：候选转移存在但被 blocker 挂起
)


def make_handler(coordinator: Coordinator) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "WorkflowCoordinator/1.0"

        def log_message(self, fmt: str, *args) -> None:  # 静音默认访问日志
            return

        def _send(self, status: int, payload: dict) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _handle(self, method: str) -> None:
            from urllib.parse import parse_qs, urlparse

            parsed = urlparse(self.path)
            action = ROUTES.get((method, parsed.path))
            if action is None:
                self._send(404, {"ok": False, "error": {
                    "type": "NotFound", "message": f"无此接口：{method} {self.path}"}})
                return
            payload: dict = {}
            if method == "POST":
                try:
                    raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                    payload = json.loads(raw or b"{}")
                    if not isinstance(payload, dict):
                        raise ValueError
                except (ValueError, json.JSONDecodeError):
                    self._send(400, {"ok": False, "error": {
                        "type": "ValidationError", "message": "请求体必须是 JSON 对象"}})
                    return
            else:
                query = parse_qs(parsed.query)
                payload = {key: values[0] for key, values in query.items()}
            kwargs = {
                alias: payload[name]
                for name, alias in FIELD_ALIASES[action].items()
                if name in payload
            }
            try:
                result = getattr(coordinator, action)(**kwargs)
            except Exception as error:  # noqa: BLE001 - 统一错误出口
                for error_type, status in ERROR_STATUS:
                    if isinstance(error, error_type):
                        self._send(status, {"ok": False, "error": {
                            "type": type(error).__name__, "message": str(error)}})
                        return
                raise
            self._send(200, {"ok": True, "data": result})

        do_POST = lambda self: self._handle("POST")  # noqa: E731
        do_GET = lambda self: self._handle("GET")  # noqa: E731

    return Handler


def serve(db_path: str = ":memory:", host: str = "127.0.0.1", port: int = 8080) -> None:
    coordinator = Coordinator(db_path)
    httpd = ThreadingHTTPServer((host, port), make_handler(coordinator))
    print(f"工作流协调服务监听 http://{host}:{port}（db={db_path}）")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        coordinator.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="工作流协调 HTTP 服务")
    parser.add_argument("--db", default=":memory:")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    serve(args.db, args.host, args.port)
