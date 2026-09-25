"""共享产线过敏原换线放行服务的端到端测试。"""

import json
import shutil
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from changeover_core import ChangeoverService, Store
from service import Handler


def start_server(db_path=":memory:"):
    store = Store(db_path)
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.changeover = ChangeoverService(store)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, store


def stop_server(server, store):
    server.shutdown()
    server.server_close()
    store.close()


class Api:
    def __init__(self, server):
        self.base = f"http://127.0.0.1:{server.server_port}"

    def call(self, method, path, body=None):
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = Request(self.base + path, data=data, method=method)
        if data is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with urlopen(request, timeout=5) as response:
                return response.status, json.load(response)
        except HTTPError as error:
            payload = json.load(error)
            error.close()
            return error.code, payload

    def post(self, path, body=None):
        return self.call("POST", path, body or {})

    def get(self, path):
        return self.call("GET", path)


# ----------------------------------------------------------------------
# 布景助手(模块级,恢复测试与主测试类共用)
# ----------------------------------------------------------------------


def make_batch(batch_id, material, declared, quantity=1000, recipe="RV-1.0"):
    return {
        "batch_id": batch_id,
        "line_id": "LINE-1",
        "material_id": material,
        "recipe_version": recipe,
        "label_declaration": {"declared_allergens": declared, "text": f"{batch_id} 标签"},
        "quantity": quantity,
    }


def seed_line(api):
    for material_id, name, allergens in (
        ("pea_protein", "豌豆蛋白", ["pea"]),
        ("fava_protein", "蚕豆蛋白", ["fava"]),
        ("gluten_mix", "含麸质辅料", ["gluten"]),
    ):
        status, body = api.post(
            "/materials",
            {"material_id": material_id, "name": name, "allergens": allergens},
        )
        assert status == 200, body
    status, body = api.post(
        "/lines",
        {
            "line_id": "LINE-1",
            "segments": [
                {
                    "segment_id": "MIX",
                    "cleaning_steps": [
                        {"key": "flush", "description": "拆洗冲洗"},
                        {"key": "wipe", "description": "内表面擦拭"},
                    ],
                },
                {
                    "segment_id": "EXTRUDE",
                    "cleaning_steps": [{"key": "purge", "description": "螺杆清机"}],
                },
                {
                    "segment_id": "PACK",
                    "cleaning_steps": [{"key": "vacuum", "description": "粉尘吸除"}],
                },
            ],
            "detection_limits": {"pea": 10.0, "fava": 10.0, "gluten": 20.0},
        },
    )
    assert status == 200, body


def add_batch(api, batch_id, material, declared, **kwargs):
    status, body = api.post("/batches", make_batch(batch_id, material, declared, **kwargs))
    assert status == 200, body
    return body


def set_sequence(api, batch_ids):
    status, body = api.post("/lines/LINE-1/sequence", {"batch_ids": batch_ids})
    assert status == 200, body
    return body


def generate(api):
    status, body = api.post("/lines/LINE-1/plans:generate")
    assert status == 200, body
    return body


def complete_steps(api, plan, operator="OP-1"):
    for segment in plan["segments"]:
        for step in segment["cleaning_steps"]:
            status, body = api.post(
                f"/plans/{plan['plan_id']}/segments/{segment['segment_id']}"
                f"/steps/{step['step_key']}/complete",
                {"operator_id": operator},
            )
            assert status == 200, body


def result_sample(api, sample_no, value):
    status, body = api.post(f"/samples/{sample_no}/receive", {"received_by": "LAB-RECV"})
    assert status == 200, body
    status, body = api.post(
        f"/samples/{sample_no}/result",
        {"result_value": value, "unit": "ppm", "method": "ELISA", "issued_by": "LAB-QA"},
    )
    assert status == 200, body
    return body


def complete_plan(api, plan, results=None):
    complete_steps(api, plan)
    for segment in plan["segments"]:
        for sample in segment["verification_samples"]:
            result_sample(api, sample["sample_no"], (results or {}).get(sample["sample_no"], 1.0))


def release(api, batch_id, key, qp="QP-1"):
    return api.post(
        f"/batches/{batch_id}/release", {"qp_id": qp, "idempotency_key": key}
    )


def pea_to_fava_world(api):
    """B1(豌豆) → B2/B3(蚕豆) → B4(含麸质) 的标准生产顺序。"""
    add_batch(api, "B1", "pea_protein", ["pea"])
    add_batch(api, "B2", "fava_protein", ["fava"])
    add_batch(api, "B3", "fava_protein", ["fava"])
    add_batch(api, "B4", "gluten_mix", ["gluten"])
    set_sequence(api, ["B1", "B2", "B3", "B4"])
    return generate(api)


class ChangeoverTestCase(unittest.TestCase):
    def setUp(self):
        self.server, self.store = start_server()
        self.api = Api(self.server)
        seed_line(self.api)

    def tearDown(self):
        stop_server(self.server, self.store)

    # ------------------------------------------------------------------
    # 换线计划生成
    # ------------------------------------------------------------------

    def test_plan_generated_from_real_sequence(self):
        generated = pea_to_fava_world(self.api)
        plans = generated["plans"]
        # 画像变化只有 B1→B2 与 B3→B4 两处,B2→B3 同画像不生成计划。
        self.assertEqual(len(plans), 2)
        plan = plans[0]
        self.assertEqual(plan["from_batch_id"], "B1")
        self.assertEqual(plan["to_batch_id"], "B2")
        self.assertEqual([s["segment_id"] for s in plan["segments"]], ["MIX", "EXTRUDE", "PACK"])
        for segment in plan["segments"]:
            # 按产线段登记:上一批物料、拆洗步骤、验证样本、检测限、下一批标签声明。
            self.assertEqual(segment["previous_material"], "pea_protein")
            self.assertTrue(segment["cleaning_steps"])
            self.assertEqual(
                segment["next_label_declaration"]["declared_allergens"], ["fava"]
            )
            self.assertEqual(len(segment["verification_samples"]), 1)
            sample = segment["verification_samples"][0]
            self.assertEqual(sample["allergen"], "pea")  # 下一批未声明的上一批过敏原
            self.assertEqual(sample["detection_limit"], 10.0)
        # 清洁窗口按同画像运行段划分:B2、B3 共用一个窗口。
        windows = {tuple(sorted(m["batch_id"] for m in w["members"])): w
                   for w in generated["windows"]}
        self.assertIn(("B2", "B3"), windows)
        self.assertEqual(windows[("B2", "B3")]["plan_id"], plan["plan_id"])
        self.assertEqual(windows[("B4",)]["plan_id"], plans[1]["plan_id"])

    def test_generate_is_idempotent(self):
        first = pea_to_fava_world(self.api)
        second = generate(self.api)
        self.assertEqual([p["plan_id"] for p in first["plans"]],
                         [p["plan_id"] for p in second["plans"]])
        self.assertEqual(second["affected"]["new_plans"], [])
        self.assertEqual(second["affected"]["superseded_plans"], [])

    # ------------------------------------------------------------------
    # 放行门槛
    # ------------------------------------------------------------------

    def test_release_blocked_until_coverage_complete(self):
        generated = pea_to_fava_world(self.api)
        status, body = release(self.api, "B2", "REL-KEY-1")
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "COVERAGE_INCOMPLETE")
        self.assertTrue(body["error"]["details"]["missing_steps"])
        self.assertTrue(body["error"]["details"]["missing_samples"])
        # 完成清洁与检测后放行成功。
        complete_plan(self.api, generated["plans"][0])
        status, body = release(self.api, "B2", "REL-KEY-1")
        self.assertEqual(status, 200, body)
        self.assertEqual(body["recipe_version"], "RV-1.0")
        self.assertEqual(body["label_snapshot"]["declared_allergens"], ["fava"])
        self.assertFalse(body["replayed"])

    def test_release_transactional_for_whole_window(self):
        generated = pea_to_fava_world(self.api)
        complete_plan(self.api, generated["plans"][0])
        status, release_b2 = release(self.api, "B2", "KEY-B2")
        self.assertEqual(status, 200, release_b2)
        # 同一清洁窗口的 B3 复用同一份清洁结论。
        status, release_b3 = release(self.api, "B3", "KEY-B3")
        self.assertEqual(status, 200, release_b3)
        self.assertEqual(release_b2["conclusion_id"], release_b3["conclusion_id"])
        status, window = self.api.get(f"/windows/{release_b2['window_id']}")
        self.assertEqual(window["status"], "CONCLUDED_PASS")
        self.assertEqual(window["conclusion"]["outcome"], "PASS")

    def test_detection_limit_boundary(self):
        generated = pea_to_fava_world(self.api)
        plan = generated["plans"][0]
        samples = [s for seg in plan["segments"] for s in seg["verification_samples"]]
        # 等于检测限判定合格,超过检测限判定失败。
        boundary = result_sample(self.api, samples[0]["sample_no"], 10.0)
        self.assertEqual(boundary["outcome"], "PASS")
        over = result_sample(self.api, samples[1]["sample_no"], 10.1)
        self.assertEqual(over["outcome"], "FAIL")

    # ------------------------------------------------------------------
    # 样本重传:复用与冻结
    # ------------------------------------------------------------------

    def test_identical_retransmission_reuses_result(self):
        generated = pea_to_fava_world(self.api)
        plan = generated["plans"][0]
        sample = plan["segments"][0]["verification_samples"][0]
        first = result_sample(self.api, sample["sample_no"], 3.0)
        self.assertFalse(first["reused"])
        status, body = self.api.post(
            f"/samples/{sample['sample_no']}/result",
            {"result_value": 3.0, "unit": "ppm", "method": "ELISA", "issued_by": "LAB-QA"},
        )
        self.assertEqual(status, 200)
        self.assertTrue(body["reused"])
        self.assertEqual(body["result_value"], 3.0)

    def test_conflicting_retransmission_freezes_window(self):
        generated = pea_to_fava_world(self.api)
        plan = generated["plans"][0]
        sample = plan["segments"][0]["verification_samples"][0]
        result_sample(self.api, sample["sample_no"], 3.0)
        status, body = self.api.post(
            f"/samples/{sample['sample_no']}/result",
            {"result_value": 9.9, "unit": "ppm", "method": "ELISA", "issued_by": "LAB-QA"},
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "SAMPLE_RESULT_CONFLICT")
        window_id = body["error"]["details"]["window_id"]
        status, window = self.api.get(f"/windows/{window_id}")
        self.assertEqual(window["status"], "FROZEN")
        # 即使其余覆盖全部完成,冻结窗口内批次也不能放行。
        complete_steps(self.api, plan)
        for segment in plan["segments"]:
            for other in segment["verification_samples"]:
                if other["sample_no"] != sample["sample_no"]:
                    result_sample(self.api, other["sample_no"], 1.0)
        status, body = release(self.api, "B2", "KEY-FROZEN")
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "WINDOW_FROZEN")

    def test_result_requires_receive_and_known_sample(self):
        generated = pea_to_fava_world(self.api)
        sample = generated["plans"][0]["segments"][0]["verification_samples"][0]
        status, body = self.api.post(
            f"/samples/{sample['sample_no']}/result",
            {"result_value": 1.0, "unit": "ppm", "method": "ELISA", "issued_by": "LAB-QA"},
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "SAMPLE_NOT_RECEIVED")
        status, body = self.api.post("/samples/SMP-NOPE/receive", {"received_by": "LAB-RECV"})
        self.assertEqual(status, 404)

    # ------------------------------------------------------------------
    # 抽样失败:隔离与已出厂风险处置
    # ------------------------------------------------------------------

    def test_failed_sample_isolates_unshipped_window_batches(self):
        generated = pea_to_fava_world(self.api)
        plan = generated["plans"][0]
        sample = plan["segments"][0]["verification_samples"][0]
        complete_steps(self.api, plan)
        failing = result_sample(self.api, sample["sample_no"], 50.0)
        self.assertEqual(failing["outcome"], "FAIL")
        # 同一清洁窗口内尚未出厂的 B2、B3 都被隔离。
        self.assertEqual(sorted(failing["affected"]["isolated_batches"]), ["B2", "B3"])
        for batch_id in ("B2", "B3"):
            status, batch = self.api.get(f"/batches/{batch_id}")
            self.assertEqual(batch["status"], "ISOLATED")
            status, body = release(self.api, batch_id, f"KEY-{batch_id}")
            self.assertEqual(status, 409)
            self.assertEqual(body["error"]["code"], "BATCH_ISOLATED")

    def test_shipped_portion_gets_traceable_disposition(self):
        generated = pea_to_fava_world(self.api)
        plan = generated["plans"][0]
        complete_plan(self.api, plan)
        status, rel = release(self.api, "B2", "KEY-B2")
        self.assertEqual(status, 200, rel)
        status, shipment = self.api.post(
            "/batches/B2/shipments", {"quantity": 400, "destination": "客户甲"}
        )
        self.assertEqual(status, 200, shipment)
        # 放行后追加确认性样本,检出超限。
        status, supplementary = self.api.post(
            f"/plans/{plan['plan_id']}/samples",
            {"sample_no": "SMP-CONFIRM-1", "segment_id": "MIX", "allergen": "pea"},
        )
        self.assertEqual(status, 200, supplementary)
        failing = result_sample(self.api, "SMP-CONFIRM-1", 88.0)
        self.assertEqual(failing["outcome"], "FAIL")
        # 窗口内未出厂余量(B2 剩余 600、B3 全部)被隔离。
        self.assertEqual(sorted(failing["affected"]["isolated_batches"]), ["B2", "B3"])
        self.assertEqual(len(failing["affected"]["dispositions"]), 1)
        # 已出厂部分生成可追踪风险处置,且指向原放行单。
        status, trace = self.api.get("/batches/B2/trace")
        self.assertEqual(status, 200)
        self.assertEqual(len(trace["risk_dispositions"]), 1)
        disposition = trace["risk_dispositions"][0]
        self.assertEqual(disposition["shipped_qty"], 400)
        self.assertEqual(disposition["trigger"], "SAMPLE_FAIL")
        self.assertEqual(disposition["original_release_id"], rel["release_id"])
        targets = {(n["type"], n["target"]) for n in disposition["notifications"]}
        self.assertIn(("CUSTOMER", "客户甲"), targets)
        # 原检验/放行记录不被改写。
        self.assertEqual(trace["release"]["release_id"], rel["release_id"])
        self.assertEqual(trace["release"]["recipe_version"], "RV-1.0")
        self.assertEqual(trace["release"]["label_snapshot"]["declared_allergens"], ["fava"])

    # ------------------------------------------------------------------
    # 返工 / 设备旁路 / 计划外插单
    # ------------------------------------------------------------------

    def test_unplanned_insertion_recomputes_scope(self):
        add_batch(self.api, "B1", "pea_protein", ["pea"])
        add_batch(self.api, "B4", "gluten_mix", ["gluten"])
        set_sequence(self.api, ["B1", "B4"])
        generated = generate(self.api)
        self.assertEqual(len(generated["plans"]), 1)
        original_plan = generated["plans"][0]["plan_id"]
        status, event = self.api.post(
            "/lines/LINE-1/events",
            {
                "type": "UNPLANNED_INSERTION",
                "after_batch_id": "B1",
                "batch": make_batch("B9", "fava_protein", ["fava"]),
            },
        )
        self.assertEqual(status, 200, event)
        affected = event["affected"]
        self.assertIn(original_plan, affected["superseded_plans"])
        self.assertEqual(len(affected["new_plans"]), 2)  # B1→B9 与 B9→B4
        regenerated = generate(self.api)
        transitions = {(p["from_batch_id"], p["to_batch_id"]) for p in regenerated["plans"]}
        self.assertEqual(transitions, {("B1", "B9"), ("B9", "B4")})

    def test_equipment_bypass_recomputes_plan(self):
        generated = pea_to_fava_world(self.api)
        plan = generated["plans"][0]
        self.assertEqual(len(plan["segments"]), 3)
        status, event = self.api.post(
            "/lines/LINE-1/events",
            {"type": "EQUIPMENT_BYPASS", "batch_id": "B2", "segment_id": "PACK"},
        )
        self.assertEqual(status, 200, event)
        self.assertIn(plan["plan_id"], event["affected"]["superseded_plans"])
        self.assertEqual(len(event["affected"]["new_plans"]), 1)
        status, new_plan = self.api.get(f"/plans/{event['affected']['new_plans'][0]}")
        # 旁路后 PACK 段不再纳入 B2 的换线覆盖范围。
        self.assertEqual([s["segment_id"] for s in new_plan["segments"]], ["MIX", "EXTRUDE"])

    def test_rework_recomputes_scope(self):
        add_batch(self.api, "B1", "pea_protein", ["pea"])
        add_batch(self.api, "B2", "fava_protein", ["fava"])
        set_sequence(self.api, ["B1", "B2"])
        generate(self.api)
        status, event = self.api.post(
            "/lines/LINE-1/events", {"type": "REWORK", "batch_id": "B1"}
        )
        self.assertEqual(status, 200, event)
        # 返工批次重新上线,新增 B2→B1#2 的换线计划与清洁窗口。
        self.assertEqual(len(event["affected"]["new_plans"]), 1)
        self.assertEqual(len(event["affected"]["new_windows"]), 1)
        status, plan = self.api.get(f"/plans/{event['affected']['new_plans'][0]}")
        self.assertEqual(plan["from_batch_id"], "B2")
        self.assertEqual(plan["to_batch_id"], "B1")
        self.assertEqual(plan["to_occurrence"], "B1@2")

    # ------------------------------------------------------------------
    # 例外批准与追溯
    # ------------------------------------------------------------------

    def test_exception_allows_isolated_release(self):
        generated = pea_to_fava_world(self.api)
        plan = generated["plans"][0]
        samples = [s for seg in plan["segments"] for s in seg["verification_samples"]]
        failing_sample = samples[0]["sample_no"]
        complete_steps(self.api, plan)
        result_sample(self.api, failing_sample, 50.0)
        for sample in samples[1:]:
            result_sample(self.api, sample["sample_no"], 1.0)
        status, body = release(self.api, "B2", "KEY-ISO")
        self.assertEqual(status, 409)
        # 质量授权人例外批准:豁免失败样本并允许隔离批次放行。
        status, exception = self.api.post(
            "/batches/B2/exceptions",
            {
                "qp_id": "QP-1",
                "reason": "失败样本对应产线段已物理隔离复验,其余证据充分",
                "waived_samples": [failing_sample],
                "allow_isolated_release": True,
            },
        )
        self.assertEqual(status, 200, exception)
        status, rel = release(self.api, "B2", "KEY-ISO")
        self.assertEqual(status, 200, rel)
        self.assertEqual(rel["exception_ids"], [exception["exception_id"]])
        status, trace = self.api.get("/batches/B2/trace")
        self.assertEqual(trace["exceptions"][0]["exception_id"], exception["exception_id"])
        self.assertEqual(trace["responsibility"]["exceptions_approved_by"], ["QP-1"])

    def test_trace_links_responsibility_evidence_and_notifications(self):
        generated = pea_to_fava_world(self.api)
        plan = generated["plans"][0]
        complete_plan(self.api, plan)
        status, rel = release(self.api, "B2", "KEY-B2", qp="QP-9")
        self.assertEqual(status, 200, rel)
        self.api.post("/batches/B2/shipments", {"quantity": 100, "destination": "客户乙"})
        self.api.post(
            f"/plans/{plan['plan_id']}/samples",
            {"sample_no": "SMP-CONFIRM-9", "segment_id": "PACK", "allergen": "pea"},
        )
        result_sample(self.api, "SMP-CONFIRM-9", 30.0)
        status, trace = self.api.get("/batches/B2/trace")
        self.assertEqual(status, 200)
        # 换线责任:操作员、实验室、质量授权人。
        responsibility = trace["responsibility"]
        self.assertEqual(responsibility["operators"], ["OP-1"])
        self.assertEqual(responsibility["lab_received_by"], ["LAB-RECV"])
        self.assertEqual(responsibility["lab_result_by"], ["LAB-QA"])
        self.assertEqual(responsibility["released_by"], "QP-9")
        # 样本证据可逐条反查。
        evidence = [
            s
            for w in trace["windows"]
            for seg in w.get("plan", {}).get("segments", [])
            for s in seg["verification_samples"]
        ]
        self.assertTrue(any(s["sample_no"] == "SMP-CONFIRM-9" and s["outcome"] == "FAIL"
                            for s in evidence))
        # 通知范围覆盖已出厂客户与内部部门。
        targets = {(n["type"], n["target"]) for n in trace["notification_scope"]}
        self.assertIn(("CUSTOMER", "客户乙"), targets)
        self.assertIn(("INTERNAL", "quality"), targets)

    # ------------------------------------------------------------------
    # 出厂约束
    # ------------------------------------------------------------------

    def test_shipment_requires_release_and_within_quantity(self):
        generated = pea_to_fava_world(self.api)
        status, body = self.api.post(
            "/batches/B2/shipments", {"quantity": 10, "destination": "客户甲"}
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "BATCH_NOT_RELEASED")
        complete_plan(self.api, generated["plans"][0])
        release(self.api, "B2", "KEY-B2")
        status, body = self.api.post(
            "/batches/B2/shipments", {"quantity": 2000, "destination": "客户甲"}
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "SHIPMENT_EXCEEDS_QUANTITY")


class ReleaseRecoveryTest(unittest.TestCase):
    """服务恢复后不能重复放行:重开同一数据文件,重试仍是原放行单。"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="changeover-")
        self.db_path = f"{self.tmpdir}/changeover.sqlite3"

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_release_survives_restart_without_duplicates(self):
        server, store = start_server(self.db_path)
        api = Api(server)
        seed_line(api)
        add_batch(api, "B1", "pea_protein", ["pea"])
        add_batch(api, "B2", "fava_protein", ["fava"])
        set_sequence(api, ["B1", "B2"])
        generated = generate(api)
        complete_plan(api, generated["plans"][0])
        status, rel = api.post(
            "/batches/B2/release", {"qp_id": "QP-1", "idempotency_key": "KEY-RECOVERY"}
        )
        self.assertEqual(status, 200, rel)
        stop_server(server, store)

        # “服务恢复”:同一数据文件重新起服务。
        server2, store2 = start_server(self.db_path)
        try:
            api2 = Api(server2)
            status, replay = api2.post(
                "/batches/B2/release", {"qp_id": "QP-1", "idempotency_key": "KEY-RECOVERY"}
            )
            self.assertEqual(status, 200, replay)
            self.assertTrue(replay["replayed"])
            self.assertEqual(replay["release_id"], rel["release_id"])
            # 即使换了幂等键,同一批次也只有一张放行单。
            status, replay2 = api2.post(
                "/batches/B2/release", {"qp_id": "QP-2", "idempotency_key": "KEY-OTHER"}
            )
            self.assertEqual(status, 200, replay2)
            self.assertEqual(replay2["release_id"], rel["release_id"])
            status, trace = api2.get("/batches/B2/trace")
            self.assertEqual(trace["release"]["release_id"], rel["release_id"])
            status, batch = api2.get("/batches/B2")
            self.assertEqual(batch["status"], "RELEASED")
        finally:
            stop_server(server2, store2)


if __name__ == "__main__":
    unittest.main()
