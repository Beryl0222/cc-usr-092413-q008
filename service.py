"""豆类结构配方放大的基础运行入口，叠加共享产线过敏原换线放行接口。"""

import argparse
import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from changeover import ChangeoverService, DomainError, Store, Validation

SERVICE_ID = "legume-formula-scaleup"
SERVICE_NAME = "豆类结构配方放大"


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


def _call(method):
    def handler(service, groups, body):
        return getattr(service, method)(*groups, **body)

    return handler


# 领域路由：换线计划只能经由生产顺序生成，不提供手工建计划入口。
ROUTES = [
    ("POST", r"materials", _call("register_material")),
    ("POST", r"formula-versions", _call("register_formula_version")),
    ("POST", r"lines", _call("register_line")),
    ("GET", r"lines/([^/]+)/plans", _call("list_plans")),
    ("POST", r"lines/([^/]+)/sequence", _call("append_sequence")),
    ("POST", r"lines/([^/]+)/sequence:insert", _call("insert_sequence")),
    ("POST", r"lines/([^/]+)/plans:generate", _call("generate_plans")),
    ("GET", r"plans/([^/]+)", _call("get_plan")),
    ("POST", r"plans/([^/]+)/steps/([^/]+)/complete", _call("complete_step")),
    ("POST", r"plans/([^/]+)/samples", _call("register_sample")),
    ("POST", r"plans/([^/]+)/segments/([^/]+)/bypass", _call("bypass_segment")),
    ("POST", r"samples/([^/]+)/receipt", _call("receive_sample")),
    ("POST", r"samples/([^/]+)/results", _call("submit_result")),
    ("POST", r"batches/([^/]+)/complete", _call("complete_batch")),
    ("POST", r"batches/([^/]+)/release", _call("release_batch")),
    ("POST", r"batches/([^/]+)/ship", _call("ship_batch")),
    ("POST", r"batches/([^/]+)/rework", _call("rework_batch")),
    ("GET", r"batches/([^/]+)/trace", _call("trace_batch")),
    ("POST", r"isolations/([^/]+)/exception-approvals", _call("approve_exception")),
    ("POST", r"dispositions/([^/]+)/notifications", _call("add_notification")),
    ("GET", r"windows/([^/]+)", _call("get_window")),
]


class Handler(BaseHTTPRequestHandler):
    """提供健康检查，并为领域接口保留清晰入口。"""

    service = None

    @classmethod
    def get_service(cls):
        if cls.service is None:
            cls.service = ChangeoverService(Store(os.environ.get("CHANGEOVER_DB", "changeover.db")))
        return cls.service

    def do_GET(self):
        if self.path == "/health":
            self._send_json(200, health_payload())
            return
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def _dispatch(self, method):
        path = self.path.split("?", 1)[0].strip("/")
        for route_method, pattern, handler in ROUTES:
            if route_method != method:
                continue
            match = re.fullmatch(pattern, path)
            if match is None:
                continue
            try:
                body = self._read_json() if method == "POST" else {}
                result = handler(self.get_service(), match.groups(), body)
                self._send_json(200, result)
            except DomainError as error:
                self._send_json(error.status, {"error": error.code, "message": str(error)})
            except Exception as error:  # 服务边界兜底，不外泄内部细节
                self._send_json(500, {"error": "internal", "message": str(error)})
            return
        self.send_error(404)

    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as error:
            raise Validation("请求体不是合法 JSON") from error
        if not isinstance(data, dict):
            raise Validation("请求体必须是 JSON 对象")
        return data

    def _send_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        return


def main():
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--db", default=os.environ.get("CHANGEOVER_DB", "changeover.db"))
    args = parser.parse_args()
    if args.check:
        assert health_payload()["service"] == SERVICE_ID
        ChangeoverService(Store(":memory:"))
        print("基础检查通过")
        return
    Handler.service = ChangeoverService(Store(args.db))
    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
