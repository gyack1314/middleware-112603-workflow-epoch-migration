"""HTTP 适配层冒烟测试：不同版本执行端通过项目接口领取与回执。"""

import json
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from workflow_coordinator.http_server import make_handler
from workflow_coordinator.engine import Coordinator


def post(url: str, payload: dict) -> tuple[int, dict]:
    request = urllib.request.Request(
        url, data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


def get(url: str) -> tuple[int, dict]:
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.status, json.loads(response.read())


class HttpApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.coordinator = Coordinator(":memory:")
        handler = make_handler(self.coordinator)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.coordinator.close()

    def test_full_lifecycle_over_http(self):
        status, body = post(f"{self.base}/definitions", {
            "version": 1,
            "steps": [{"name": "approve", "kind": "signal", "needs": []}],
        })
        self.assertEqual(status, 200)
        status, body = post(f"{self.base}/definitions", {
            "version": 2,
            "steps": [
                {"name": "check", "kind": "signal", "needs": []},
                {"name": "publish", "kind": "signal", "needs": ["check"]},
            ],
        })
        self.assertEqual(status, 200)
        status, body = post(f"{self.base}/compat", {
            "from": 1, "to": 2,
            "steps": {"approve": {"step": "check", "reuse": "partial",
                                  "reason": "检查通过"}},
        })
        self.assertEqual(status, 200)
        status, _ = post(f"{self.base}/instances", {"instance": "x", "version": 1})
        self.assertEqual(status, 200)
        status, _ = post(f"{self.base}/signals", {
            "instance": "x", "version": 1, "step": "approve",
            "id": "s1", "reason": "检查通过",
        })
        self.assertEqual(status, 200)
        # publish 是新版新增步骤，首次转移只能挂起为候选（423）。
        status, body = post(f"{self.base}/transfers",
                            {"instance": "x", "request": "m", "to": 2})
        self.assertEqual(status, 423)
        # 新版信号在候选期间预登记，重估后原子提交。
        status, _ = post(f"{self.base}/signals", {
            "instance": "x", "version": 2, "step": "publish", "id": "s2",
        })
        self.assertEqual(status, 200)
        # 值班查询确认已切换。
        status, body = get(f"{self.base}/instances?instance=x")
        self.assertEqual(status, 200)
        self.assertEqual(body["data"]["effective_version"], 2)
        self.assertEqual(body["data"]["active_transfer"], None)
        self.assertEqual(body["data"]["waiting_evidence"], [])

    def test_blocked_transfer_maps_to_423_and_conflict_to_409(self):
        post(f"{self.base}/definitions", {
            "version": 1,
            "steps": [{"name": "approve", "kind": "signal", "needs": []}],
        })
        post(f"{self.base}/definitions", {
            "version": 2,
            "steps": [{"name": "check", "kind": "signal", "needs": []}],
        })
        post(f"{self.base}/compat", {"from": 1, "to": 2, "steps": {}})
        post(f"{self.base}/instances", {"instance": "x", "version": 1})
        status, body = post(f"{self.base}/transfers",
                            {"instance": "x", "request": "m1", "to": 2})
        self.assertEqual(status, 423)  # 候选挂起：缺映射输入
        status, body = post(f"{self.base}/transfers",
                            {"instance": "x", "request": "m2", "to": 2})
        self.assertEqual(status, 409)  # 同时只能有一个接管
        status, body = get(f"{self.base}/instances?instance=x")
        self.assertEqual(body["data"]["active_transfer"]["request_id"], "m1")


if __name__ == "__main__":
    unittest.main()
