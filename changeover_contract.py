"""共享产线过敏原换线放行服务的领域契约测试。"""

import json
import os
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import service
from changeover import (
    BATCH_COMPLETED,
    BATCH_ISOLATED,
    BATCH_PLANNED,
    BATCH_RELEASED,
    BATCH_SHIPPED,
    CONCLUSION_EXCEPTION,
    CONCLUSION_PASS,
    PLAN_CONCLUDED,
    PLAN_FAILED,
    PLAN_FROZEN,
    PLAN_INVALID,
    PLAN_OPEN,
    PLAN_SUPERSEDED,
    RELEASE_ACTIVE,
    RELEASE_SUPERSEDED,
    SAMPLE_FROZEN,
    VERDICT_FAIL,
    VERDICT_PASS,
    ChangeoverService,
    Conflict,
    NotFound,
    Store,
    Validation,
)


def make_service(store=None):
    """搭建豌豆/蚕豆/含麸质辅料轮换的标准中试产线。"""
    svc = ChangeoverService(store or Store(":memory:"))
    svc.register_material(id="pea-protein", name="豌豆蛋白", allergens=["pea"])
    svc.register_material(id="fava-protein", name="蚕豆蛋白", allergens=["fava_bean"])
    svc.register_material(id="wheat-starch", name="小麦淀粉", allergens=["gluten"])
    svc.register_material(id="water", name="工艺水", allergens=[])
    svc.register_formula_version(
        id="FV-PEA", materials=["pea-protein", "water"], label_declarations=["pea"]
    )
    svc.register_formula_version(
        id="FV-FAVA", materials=["fava-protein", "water"], label_declarations=["fava_bean"]
    )
    svc.register_formula_version(
        id="FV-GLUTEN", materials=["wheat-starch", "water"], label_declarations=["gluten"]
    )
    svc.register_line(
        id="L1",
        name="结构化中试线",
        segments=[
            {
                "id": "SEG-MIX",
                "name": "混合段",
                "steps": ["拆卸", "冲洗", "目视检查"],
                "detection_limits": {"pea": 10.0, "fava_bean": 10.0, "gluten": 20.0},
            },
            {
                "id": "SEG-FREEZE",
                "name": "冷冻段",
                "steps": ["冲洗"],
                "detection_limits": {"pea": 15.0, "fava_bean": 15.0, "gluten": 25.0},
            },
        ],
    )
    return svc


def append(svc, batch_id, formula):
    return svc.append_sequence("L1", batch_id=batch_id, formula_version_id=formula)


def plan_between(svc, from_batch, to_batch):
    for plan in svc.list_plans("L1"):
        if plan["from_batch_id"] == from_batch and plan["to_batch_id"] == to_batch:
            return plan
    return None


def cover_plan(svc, plan_id, value=1.0, operator="op-1", lab="lab-1"):
    """操作员完成全部拆洗步骤，实验室签收并出具合格结果。"""
    plan = svc.get_plan(plan_id)
    for segment in plan["segments"]:
        for step in segment["steps"]:
            svc.complete_step(plan_id, step["id"], operator=operator)
        for sample in segment["samples"]:
            svc.receive_sample(sample["sampling_no"], received_by=lab)
            svc.submit_result(sample["sampling_no"], result_value=value, result_by=lab)
    return svc.get_plan(plan_id)


def batch_status(svc, batch_id):
    return svc.trace_batch(batch_id)["batch"]["status"]


class PlanGenerationTest(unittest.TestCase):
    def setUp(self):
        self.svc = make_service()

    def test_plans_are_generated_from_real_production_sequence(self):
        append(self.svc, "B1", "FV-PEA")
        append(self.svc, "B2", "FV-GLUTEN")
        append(self.svc, "B3", "FV-PEA")
        append(self.svc, "B4", "FV-PEA")
        result = self.svc.generate_plans("L1")

        # 豌豆→含麸质：风险是未声明的 pea；含麸质→豌豆：风险是未声明的 gluten
        p1 = plan_between(self.svc, "B1", "B2")
        p2 = plan_between(self.svc, "B2", "B3")
        self.assertIsNotNone(p1)
        self.assertIsNotNone(p2)
        self.assertEqual(p1["risk_allergens"], ["pea"])
        self.assertEqual(p2["risk_allergens"], ["gluten"])
        # 豌豆→豌豆无未声明风险，不产生换线计划，B3/B4 共用同一清洁窗口
        self.assertIsNone(plan_between(self.svc, "B3", "B4"))
        self.assertEqual(len(result["plans"]), 2)
        w3 = self.svc.trace_batch("B3")["window"]["id"]
        w4 = self.svc.trace_batch("B4")["window"]["id"]
        self.assertEqual(w3, w4)
        # 重复生成幂等，不产生重复计划
        again = self.svc.generate_plans("L1")
        self.assertEqual(again["new_plans"], [])
        self.assertEqual(len(self.svc.list_plans("L1")), 2)

    def test_plan_registers_segment_details(self):
        append(self.svc, "B1", "FV-PEA")
        append(self.svc, "B2", "FV-GLUTEN")
        plan = self.svc.get_plan(plan_between(self.svc, "B1", "B2")["id"])
        self.assertEqual(plan["status"], PLAN_OPEN)
        self.assertEqual([s["segment_id"] for s in plan["segments"]], ["SEG-FREEZE", "SEG-MIX"])
        for segment in plan["segments"]:
            # 上一批物料、拆洗步骤、验证样本、检测限、下一批标签声明
            self.assertEqual(
                [m["id"] for m in segment["prev_materials"]], ["pea-protein", "water"]
            )
            self.assertEqual(segment["next_label_declarations"], ["gluten"])
            self.assertTrue(segment["steps"])
            self.assertEqual(len(segment["samples"]), 1)
            sample = segment["samples"][0]
            self.assertEqual(sample["allergen"], "pea")
            self.assertEqual(
                sample["detection_limit"], segment["detection_limits"]["pea"]
            )
        mix = next(s for s in plan["segments"] if s["segment_id"] == "SEG-MIX")
        self.assertEqual([s["name"] for s in mix["steps"]], ["拆卸", "冲洗", "目视检查"])

    def test_missing_detection_limit_blocks_plan_generation_atomically(self):
        svc = ChangeoverService(Store(":memory:"))
        svc.register_material(id="pea-protein", allergens=["pea"])
        svc.register_material(id="wheat-starch", allergens=["gluten"])
        svc.register_formula_version(
            id="FV-PEA", materials=["pea-protein"], label_declarations=["pea"]
        )
        svc.register_formula_version(
            id="FV-GLUTEN", materials=["wheat-starch"], label_declarations=["gluten"]
        )
        svc.register_line(
            id="L1",
            segments=[{"id": "SEG-1", "steps": ["冲洗"], "detection_limits": {"pea": 10.0}}],
        )
        svc.append_sequence("L1", batch_id="B1", formula_version_id="FV-GLUTEN")
        # 含麸质→豌豆的风险过敏原是 gluten，产线段缺失其检测限时整个入列事务回滚
        with self.assertRaises(Validation):
            svc.append_sequence("L1", batch_id="B2", formula_version_id="FV-PEA")
        with self.assertRaises(NotFound):
            svc.trace_batch("B2")


class ReleaseFlowTest(unittest.TestCase):
    def setUp(self):
        self.svc = make_service()
        append(self.svc, "B1", "FV-PEA")
        append(self.svc, "B2", "FV-GLUTEN")
        self.plan_id = plan_between(self.svc, "B1", "B2")["id"]

    def test_release_requires_complete_coverage(self):
        self.svc.complete_batch("B2")
        with self.assertRaises(Conflict):
            self.svc.release_batch("B2", released_by="qa-1")
        plan = self.svc.get_plan(self.plan_id)
        for segment in plan["segments"]:
            for step in segment["steps"]:
                self.svc.complete_step(self.plan_id, step["id"], operator="op-1")
        with self.assertRaises(Conflict):
            self.svc.release_batch("B2", released_by="qa-1")
        for segment in plan["segments"]:
            for sample in segment["samples"]:
                self.svc.receive_sample(sample["sampling_no"], received_by="lab-1")
        with self.assertRaises(Conflict):
            self.svc.release_batch("B2", released_by="qa-1")
        # 覆盖不完整时事务整体回滚：无放行记录、计划保持 OPEN、批次仍可放行
        trace = self.svc.trace_batch("B2")
        self.assertEqual(trace["releases"], [])
        self.assertEqual(self.svc.get_plan(self.plan_id)["status"], PLAN_OPEN)
        self.assertEqual(trace["batch"]["status"], BATCH_COMPLETED)

    def test_release_commits_conclusion_version_and_label_together(self):
        cover_plan(self.svc, self.plan_id)
        self.svc.complete_batch("B2")
        release = self.svc.release_batch("B2", released_by="qa-1")
        self.assertEqual(release["cleaning_conclusion"], CONCLUSION_PASS)
        self.assertEqual(release["formula_version_id"], "FV-GLUTEN")
        self.assertEqual(release["label_snapshot"], ["gluten"])
        self.assertFalse(release["exception"])
        self.assertEqual(self.svc.get_plan(self.plan_id)["status"], PLAN_CONCLUDED)
        self.assertEqual(batch_status(self.svc, "B2"), BATCH_RELEASED)
        # 重复放行幂等：返回同一条放行记录
        again = self.svc.release_batch("B2", released_by="qa-1")
        self.assertEqual(again["id"], release["id"])

    def test_release_requires_authorized_person_and_completed_batch(self):
        with self.assertRaises(Validation):
            self.svc.release_batch("B2", released_by="")
        with self.assertRaises(Conflict):
            self.svc.release_batch("B2", released_by="qa-1")  # 尚未完工

    def test_threshold_boundary_and_result_requires_receipt(self):
        plan = self.svc.get_plan(self.plan_id)
        mix = next(s for s in plan["segments"] if s["segment_id"] == "SEG-MIX")
        sample_no = mix["samples"][0]["sampling_no"]
        with self.assertRaises(Conflict):
            self.svc.submit_result(sample_no, result_value=1.0, result_by="lab-1")
        self.svc.receive_sample(sample_no, received_by="lab-1")
        # 结果等于检测限视为满足阈值
        result = self.svc.submit_result(sample_no, result_value=10.0, result_by="lab-1")
        self.assertEqual(result["verdict"], VERDICT_PASS)
        over = self.svc.register_sample(
            self.plan_id, sampling_no="EXTRA-OVER", segment_id="SEG-MIX", allergen="pea"
        )
        self.svc.receive_sample(over["sampling_no"], received_by="lab-1")
        failed = self.svc.submit_result(over["sampling_no"], result_value=10.5, result_by="lab-1")
        self.assertEqual(failed["verdict"], VERDICT_FAIL)
        self.assertEqual(self.svc.get_plan(self.plan_id)["status"], PLAN_FAILED)

    def test_release_is_not_duplicated_after_service_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "changeover.db")
            store = Store(path)
            svc = make_service(store)
            append(svc, "B1", "FV-PEA")
            append(svc, "B2", "FV-GLUTEN")
            plan_id = plan_between(svc, "B1", "B2")["id"]
            cover_plan(svc, plan_id)
            svc.complete_batch("B2")
            first = svc.release_batch("B2", released_by="qa-1")
            store.close()
            # 模拟服务恢复：同一数据库文件重新装配
            recovered = ChangeoverService(Store(path))
            second = recovered.release_batch("B2", released_by="qa-1")
            self.assertEqual(first["id"], second["id"])
            releases = recovered.trace_batch("B2")["releases"]
            self.assertEqual(len([r for r in releases if r["status"] == RELEASE_ACTIVE]), 1)


class FailureAndFreezeTest(unittest.TestCase):
    def setUp(self):
        self.svc = make_service()
        for batch, formula in [("B1", "FV-PEA"), ("B2", "FV-GLUTEN"),
                               ("B3", "FV-PEA"), ("B4", "FV-PEA")]:
            append(self.svc, batch, formula)
        self.p1 = plan_between(self.svc, "B1", "B2")["id"]
        self.p2 = plan_between(self.svc, "B2", "B3")["id"]

    def test_failed_sample_isolates_unshipped_and_dispositions_shipped(self):
        cover_plan(self.svc, self.p1)
        self.svc.complete_batch("B2")
        self.svc.release_batch("B2", released_by="qa-1")
        self.svc.ship_batch("B2", shipped_by="logistics-1")
        cover_plan(self.svc, self.p2)
        self.svc.complete_batch("B3")
        self.svc.release_batch("B3", released_by="qa-1")
        self.svc.ship_batch("B3", shipped_by="logistics-1")
        self.svc.complete_batch("B4")
        # 同一清洁窗口内迟到的不合格结果
        extra = self.svc.register_sample(
            self.p2, sampling_no="EXTRA-1", segment_id="SEG-MIX", allergen="gluten"
        )
        self.svc.receive_sample("EXTRA-1", received_by="lab-1")
        failed = self.svc.submit_result("EXTRA-1", result_value=999.0, result_by="lab-1")
        self.assertEqual(failed["verdict"], VERDICT_FAIL)
        self.assertEqual(self.svc.get_plan(self.p2)["status"], PLAN_FAILED)
        # 未出厂批次被隔离，已出厂批次保持出厂但生成风险处置
        self.assertEqual(batch_status(self.svc, "B4"), BATCH_ISOLATED)
        self.assertEqual(batch_status(self.svc, "B3"), BATCH_SHIPPED)
        trace_b3 = self.svc.trace_batch("B3")
        self.assertEqual(len(trace_b3["dispositions"]), 1)
        snapshot = trace_b3["dispositions"][0]["inspection_snapshot"]
        self.assertEqual(snapshot["sampling_no"], "EXTRA-1")
        self.assertEqual(snapshot["result_value"], 999.0)
        self.assertEqual(snapshot["verdict"], VERDICT_FAIL)
        # 另一窗口的 B2 不受牵连
        self.assertEqual(self.svc.trace_batch("B2")["dispositions"], [])
        # 隔离批次不能直接放行
        with self.assertRaises(Conflict):
            self.svc.release_batch("B4", released_by="qa-1")
        # 原检验不可改写：相同重传复用，异内容冻结且原值保留
        reused = self.svc.submit_result("EXTRA-1", result_value=999.0, result_by="lab-1")
        self.assertEqual(reused["result_at"], failed["result_at"])
        with self.assertRaises(Conflict):
            self.svc.submit_result("EXTRA-1", result_value=1.0, result_by="lab-1")
        plan = self.svc.get_plan(self.p2)
        extra_view = next(
            s for seg in plan["segments"] for s in seg["samples"]
            if s["sampling_no"] == "EXTRA-1"
        )
        self.assertEqual(extra_view["status"], SAMPLE_FROZEN)
        self.assertEqual(extra_view["result_value"], 999.0)

    def test_identical_retransmission_reuses_and_conflict_freezes(self):
        self.svc.complete_batch("B2")
        plan = self.svc.get_plan(self.p1)
        mix = next(s for s in plan["segments"] if s["segment_id"] == "SEG-MIX")
        freeze = next(s for s in plan["segments"] if s["segment_id"] == "SEG-FREEZE")
        s1 = mix["samples"][0]["sampling_no"]
        s2 = freeze["samples"][0]["sampling_no"]
        self.svc.receive_sample(s1, received_by="lab-1")
        first = self.svc.submit_result(s1, result_value=1.0, result_by="lab-1")
        reused = self.svc.submit_result(s1, result_value=1.0, result_by="lab-1")
        self.assertEqual(reused["result_at"], first["result_at"])
        # 同一采样编号异内容重传 → 立即冻结并收紧范围
        with self.assertRaises(Conflict):
            self.svc.submit_result(s1, result_value=9.9, result_by="lab-1")
        self.assertEqual(self.svc.get_plan(self.p1)["status"], PLAN_FROZEN)
        self.assertEqual(batch_status(self.svc, "B2"), BATCH_ISOLATED)
        # 登记环节同样适用：同内容复用，异内容冻结
        same = self.svc.register_sample(
            self.p1, sampling_no=s2, segment_id="SEG-FREEZE", allergen="pea",
            detection_limit=15.0,
        )
        self.assertEqual(same["sampling_no"], s2)
        with self.assertRaises(Conflict):
            self.svc.register_sample(
                self.p1, sampling_no=s2, segment_id="SEG-FREEZE", allergen="pea",
                detection_limit=5.0,
            )
        plan = self.svc.get_plan(self.p1)
        s2_view = next(
            s for seg in plan["segments"] for s in seg["samples"] if s["sampling_no"] == s2
        )
        self.assertEqual(s2_view["status"], SAMPLE_FROZEN)

    def test_exception_approval_releases_isolated_batch(self):
        self.svc.complete_batch("B2")
        plan = self.svc.get_plan(self.p1)
        sample_no = plan["segments"][0]["samples"][0]["sampling_no"]
        self.svc.receive_sample(sample_no, received_by="lab-1")
        self.svc.submit_result(sample_no, result_value=50.0, result_by="lab-1")
        self.assertEqual(batch_status(self.svc, "B2"), BATCH_ISOLATED)
        with self.assertRaises(Conflict):
            self.svc.release_batch("B2", released_by="qa-1")
        isolation = self.svc.trace_batch("B2")["isolations"][0]
        with self.assertRaises(Validation):
            self.svc.approve_exception(isolation["id"], approved_by="qa-1", reason="")
        release = self.svc.approve_exception(
            isolation["id"], approved_by="qa-1", reason="复验合格，偏差可接受"
        )
        self.assertTrue(release["exception"])
        self.assertEqual(release["cleaning_conclusion"], CONCLUSION_EXCEPTION)
        self.assertEqual(release["released_by"], "qa-1")
        self.assertEqual(batch_status(self.svc, "B2"), BATCH_RELEASED)
        updated = self.svc.trace_batch("B2")["isolations"][0]
        self.assertEqual(updated["status"], "EXCEPTION_APPROVED")
        self.assertEqual(updated["exception_by"], "qa-1")
        self.svc.ship_batch("B2", shipped_by="logistics-1")
        self.assertEqual(batch_status(self.svc, "B2"), BATCH_SHIPPED)


class ScopeRecalculationTest(unittest.TestCase):
    def setUp(self):
        self.svc = make_service()

    def test_rework_recalculates_affected_scope(self):
        append(self.svc, "B1", "FV-PEA")
        append(self.svc, "B2", "FV-GLUTEN")
        append(self.svc, "B3", "FV-PEA")
        p1 = plan_between(self.svc, "B1", "B2")["id"]
        p2 = plan_between(self.svc, "B2", "B3")["id"]
        cover_plan(self.svc, p1)
        cover_plan(self.svc, p2)
        self.svc.complete_batch("B2")
        self.svc.release_batch("B2", released_by="qa-1")
        self.svc.complete_batch("B3")
        self.svc.release_batch("B3", released_by="qa-1")
        result = self.svc.rework_batch("B2", reason="质构不合格返工")
        # 返工批次回到队尾待产，原放行作废
        self.assertEqual(result["batch"]["status"], BATCH_PLANNED)
        self.assertEqual(result["batch"]["sequence"], 3)
        # 顺序变为 B1,B3,B2：旧计划作废，B3→B2 生成新计划
        plans = {p["id"]: p for p in self.svc.list_plans("L1")}
        self.assertEqual(plans[p1]["status"], PLAN_SUPERSEDED)
        self.assertEqual(plans[p2]["status"], PLAN_SUPERSEDED)
        p3 = plan_between(self.svc, "B3", "B2")
        self.assertIsNotNone(p3)
        self.assertEqual(p3["risk_allergens"], ["pea"])
        # B3 的放行依据已失效，被纳入受影响范围并隔离
        self.assertIn("B3", result["rebuild"]["affected_batches"])
        self.assertEqual(batch_status(self.svc, "B3"), BATCH_ISOLATED)
        # B2 需重新完工并按新计划放行，历史放行保留为作废记录
        self.svc.complete_batch("B2")
        with self.assertRaises(Conflict):
            self.svc.release_batch("B2", released_by="qa-1")
        cover_plan(self.svc, p3["id"])
        new_release = self.svc.release_batch("B2", released_by="qa-1")
        releases = self.svc.trace_batch("B2")["releases"]
        self.assertEqual(len(releases), 2)
        self.assertEqual(
            {r["status"] for r in releases}, {RELEASE_SUPERSEDED, RELEASE_ACTIVE}
        )
        self.assertEqual(new_release["plan_id"], p3["id"])

    def test_unscheduled_insertion_recalculates_affected_scope(self):
        append(self.svc, "B1", "FV-PEA")
        append(self.svc, "B2", "FV-GLUTEN")
        p1 = plan_between(self.svc, "B1", "B2")["id"]
        cover_plan(self.svc, p1)
        self.svc.complete_batch("B2")
        self.svc.release_batch("B2", released_by="qa-1")
        result = self.svc.insert_sequence(
            "L1", batch_id="BX", formula_version_id="FV-FAVA", position=1
        )
        # 旧邻接关系被打断：B1→B2 作废，新增 B1→BX 与 BX→B2
        plans = {p["id"]: p for p in self.svc.list_plans("L1")}
        self.assertEqual(plans[p1]["status"], PLAN_SUPERSEDED)
        self.assertIsNotNone(plan_between(self.svc, "B1", "BX"))
        self.assertIsNotNone(plan_between(self.svc, "BX", "B2"))
        # 已放行未出厂的 B2 清洁依据失效，被隔离并记入事件
        self.assertIn("B2", result["rebuild"]["affected_batches"])
        trace = self.svc.trace_batch("B2")
        self.assertEqual(trace["batch"]["status"], BATCH_ISOLATED)
        self.assertEqual(trace["isolations"][0]["reason"], "UNSCHEDULED_INSERTION")
        self.assertIn(
            "UNSCHEDULED_INSERTION", {e["type"] for e in trace["events"]}
        )

    def test_equipment_bypass_recalculates_affected_scope(self):
        append(self.svc, "B1", "FV-PEA")
        append(self.svc, "B2", "FV-GLUTEN")
        append(self.svc, "B3", "FV-GLUTEN")
        p1 = plan_between(self.svc, "B1", "B2")["id"]
        cover_plan(self.svc, p1)
        self.svc.complete_batch("B2")
        self.svc.release_batch("B2", released_by="qa-1")
        self.svc.complete_batch("B3")
        self.svc.release_batch("B3", released_by="qa-1")
        self.svc.ship_batch("B3", shipped_by="logistics-1")
        result = self.svc.bypass_segment(
            p1, "SEG-MIX", by="op-9", reason="旁路维修未走清洗"
        )
        self.assertEqual(self.svc.get_plan(p1)["status"], PLAN_INVALID)
        # 同窗口未出厂批次隔离，已出厂批次生成风险处置
        self.assertEqual(batch_status(self.svc, "B2"), BATCH_ISOLATED)
        self.assertEqual(batch_status(self.svc, "B3"), BATCH_SHIPPED)
        dispositions = self.svc.trace_batch("B3")["dispositions"]
        self.assertEqual(len(dispositions), 1)
        self.assertEqual(dispositions[0]["reason"], "EQUIPMENT_BYPASS")
        self.assertEqual(sorted(result["affected_batches"]), ["B2", "B3"])


class TraceabilityTest(unittest.TestCase):
    def test_finished_product_traceability(self):
        svc = make_service()
        append(svc, "B1", "FV-PEA")
        append(svc, "B2", "FV-GLUTEN")
        plan_id = plan_between(svc, "B1", "B2")["id"]
        svc.complete_batch("B2")
        plan = svc.get_plan(plan_id)
        for segment in plan["segments"]:
            for step in segment["steps"]:
                svc.complete_step(plan_id, step["id"], operator="op-1")
        sample_no = plan["segments"][0]["samples"][0]["sampling_no"]
        svc.receive_sample(sample_no, received_by="lab-1")
        svc.submit_result(sample_no, result_value=50.0, result_by="lab-1")
        isolation = svc.trace_batch("B2")["isolations"][0]
        svc.approve_exception(isolation["id"], approved_by="qa-1", reason="复验合格")
        svc.ship_batch("B2", shipped_by="logistics-1")
        # 出厂后迟到的不合格结果 → 风险处置并登记通知范围
        svc.register_sample(plan_id, sampling_no="LATE-1", segment_id="SEG-MIX",
                            allergen="pea")
        svc.receive_sample("LATE-1", received_by="lab-2")
        svc.submit_result("LATE-1", result_value=88.0, result_by="lab-2")
        disposition = svc.trace_batch("B2")["dispositions"][0]
        svc.add_notification(disposition["id"], party="经销商A", notified_by="qa-1")

        trace = svc.trace_batch("B2")
        # 换线责任：操作员、实验室、质量授权人分段留痕
        mix = next(s for s in trace["plan"]["segments"] if s["segment_id"] == "SEG-MIX")
        self.assertTrue(mix["steps"])
        self.assertTrue(all(s["completed_by"] == "op-1" for s in mix["steps"]))
        sample = next(
            s for seg in trace["plan"]["segments"] for s in seg["samples"]
            if s["sampling_no"] == sample_no
        )
        self.assertEqual(sample["received_by"], "lab-1")
        self.assertEqual(sample["result_by"], "lab-1")
        # 样本证据：原检验值未被风险处置改写
        self.assertEqual(sample["result_value"], 50.0)
        self.assertEqual(disposition["inspection_snapshot"]["result_value"], 88.0)
        # 例外批准
        self.assertEqual(trace["isolations"][0]["exception_by"], "qa-1")
        exception_release = next(r for r in trace["releases"] if r["exception"])
        self.assertEqual(exception_release["released_by"], "qa-1")
        # 通知范围
        notified = svc.trace_batch("B2")["dispositions"][0]
        self.assertEqual(notified["status"], "NOTIFIED")
        self.assertEqual(notified["notifications"][0]["party"], "经销商A")
        # 关键事件全程可查
        event_types = {e["type"] for e in trace["events"]}
        self.assertTrue(
            {"SAMPLE_FAILED", "BATCH_ISOLATED", "EXCEPTION_APPROVED", "BATCH_SHIPPED",
             "DISPOSITION_CREATED", "NOTIFICATION_SENT"} <= event_types
        )


class HttpSmokeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        service.Handler.service = make_service()
        from http.server import ThreadingHTTPServer

        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), service.Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)
        service.Handler.service = None

    def _post(self, path, payload):
        request = Request(
            f"{self.base_url}{path}",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=2) as response:
            return response.status, json.load(response)

    def test_domain_roundtrip_over_http(self):
        status, material = self._post(
            "/materials", {"id": "oat-fiber", "allergens": [], "name": "燕麦纤维"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(material["id"], "oat-fiber")
        with self.assertRaises(HTTPError) as error:
            self._post("/materials", {"id": "oat-fiber", "allergens": []})
        self.assertEqual(error.exception.code, 409)
        error.exception.close()
        with self.assertRaises(HTTPError) as error:
            urlopen(f"{self.base_url}/plans/no-such-plan", timeout=2)
        self.assertEqual(error.exception.code, 404)
        payload = json.loads(error.exception.read().decode("utf-8"))
        self.assertEqual(payload["error"], "not_found")
        error.exception.close()


if __name__ == "__main__":
    unittest.main()
