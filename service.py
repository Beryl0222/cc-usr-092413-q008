"""豆类结构配方放大的运行入口,含共享产线过敏原换线放行接口。"""

import argparse
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from changeover_core import ChangeoverService, DomainError, Store

SERVICE_ID = "legume-formula-scaleup"
SERVICE_NAME = "豆类结构配方放大"


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


def _route(method, pattern):
    """把 "/batches/{batch_id}/release" 形式的路径编译为正则。"""
    regex = re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", pattern)
    return method, re.compile(f"^{regex}$"), pattern


ROUTES = [
    (_route("POST", "/materials"), lambda svc, body, p: svc.register_material(body)),
    (_route("POST", "/lines"), lambda svc, body, p: svc.register_line(body)),
    (_route("POST", "/batches"), lambda svc, body, p: svc.register_batch(body)),
    (_route("GET", "/batches/{batch_id}"), lambda svc, body, p: svc.get_batch(p["batch_id"])),
    (_route("POST", "/lines/{line_id}/sequence"),
     lambda svc, body, p: svc.set_sequence(p["line_id"], body)),
    (_route("POST", "/lines/{line_id}/plans:generate"),
     lambda svc, body, p: svc.generate_plans(p["line_id"])),
    (_route("POST", "/lines/{line_id}/events"),
     lambda svc, body, p: svc.record_event(p["line_id"], body)),
    (_route("GET", "/plans/{plan_id}"), lambda svc, body, p: svc.get_plan(p["plan_id"])),
    (_route("POST", "/plans/{plan_id}/segments/{segment_id}/steps/{step_key}/complete"),
     lambda svc, body, p: svc.complete_step(p["plan_id"], p["segment_id"], p["step_key"], body)),
    (_route("POST", "/plans/{plan_id}/samples"),
     lambda svc, body, p: svc.add_supplementary_sample(p["plan_id"], body)),
    (_route("POST", "/samples/{sample_no}/receive"),
     lambda svc, body, p: svc.receive_sample(p["sample_no"], body)),
    (_route("POST", "/samples/{sample_no}/result"),
     lambda svc, body, p: svc.submit_sample_result(p["sample_no"], body)),
    (_route("POST", "/batches/{batch_id}/release"),
     lambda svc, body, p: svc.release_batch(p["batch_id"], body)),
    (_route("POST", "/batches/{batch_id}/shipments"),
     lambda svc, body, p: svc.ship_batch(p["batch_id"], body)),
    (_route("POST", "/batches/{batch_id}/exceptions"),
     lambda svc, body, p: svc.approve_exception(p["batch_id"], body)),
    (_route("GET", "/batches/{batch_id}/trace"),
     lambda svc, body, p: svc.trace_batch(p["batch_id"])),
    (_route("GET", "/windows/{window_id}"), lambda svc, body, p: svc.get_window(p["window_id"])),
]


class Handler(BaseHTTPRequestHandler):
    """健康检查 + 换线放行领域接口。"""

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def _dispatch(self, method):
        path = urlparse(self.path).path
        if method == "GET" and path == "/health":
            self._respond(200, health_payload())
            return
        service = getattr(self.server, "changeover", None)
        for (route_method, regex, _pattern), action in ROUTES:
            if route_method != method:
                continue
            match = regex.match(path)
            if not match:
                continue
            if service is None:
                self._respond(503, {"error": {"code": "SERVICE_NOT_READY",
                                              "message": "领域服务未挂载"}})
                return
            try:
                body = self._read_body() if method == "POST" else {}
                result = action(service, body, match.groupdict())
            except DomainError as error:
                self._respond(error.status, {"error": {"code": error.code,
                                                       "message": error.message,
                                                       "details": error.details}})
                return
            self._respond(200, result)
            return
        self._respond(404, {"error": {"code": "NOT_FOUND", "message": "路由不存在"}})

    def _read_body(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise DomainError(400, "BAD_JSON", "请求体不是合法 JSON")
        if not isinstance(payload, dict):
            raise DomainError(400, "BAD_JSON", "请求体必须是 JSON 对象")
        return payload

    def _respond(self, status, payload):
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
    parser.add_argument("--db", default="changeover.sqlite3",
                        help="换线放行数据文件,默认 changeover.sqlite3;重启后放行记录不丢失")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        assert health_payload()["service"] == SERVICE_ID
        print("基础检查通过")
        return
    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    server.changeover = ChangeoverService(Store(args.db))
    server.serve_forever()


if __name__ == "__main__":
    main()
