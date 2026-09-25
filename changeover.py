"""共享产线过敏原换线放行领域服务。

中试工厂在同一条结构化产线上轮换豌豆、蚕豆与含麸质辅料。本模块把
换线放行从纸面清场记录升级为与真实生产顺序、设备段和待放行批次
一一对应的领域流程：

* 换线计划只能由产线的真实生产顺序生成，按产线段登记上一批物料、
  拆洗步骤、清洁验证样本、检测限与下一批标签声明；
* 操作员完成拆洗步骤，实验室签收样本并出具结果，质量授权人只对
  覆盖完整且检测满足阈值的批次放行；
* 抽样失败会隔离同一清洁窗口内尚未出厂的相关批次，已出厂部分生成
  可追踪的风险处置，原检验记录不可改写；
* 返工、设备旁路与计划外插单都会重新计算受影响范围；
* 同一采样编号的相同重传复用结果，异内容立即冻结；
* 清洁结论、配方版本与标签快照在单一事务中一起生效，服务恢复后
  不会重复放行；
* 成品可反查换线责任、样本证据、例外批准及通知范围。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

# ---- 批次状态 ----
BATCH_PLANNED = "PLANNED"
BATCH_COMPLETED = "COMPLETED"
BATCH_RELEASED = "RELEASED"
BATCH_SHIPPED = "SHIPPED"
BATCH_ISOLATED = "ISOLATED"

# ---- 换线计划状态 ----
PLAN_OPEN = "OPEN"
PLAN_CONCLUDED = "CONCLUDED"
PLAN_FAILED = "FAILED"
PLAN_FROZEN = "FROZEN"
PLAN_INVALID = "INVALID"
PLAN_SUPERSEDED = "SUPERSEDED"
PLAN_BAD_STATES = {PLAN_FAILED, PLAN_FROZEN, PLAN_INVALID, PLAN_SUPERSEDED}

# ---- 样本状态 ----
SAMPLE_REGISTERED = "REGISTERED"
SAMPLE_RECEIVED = "RECEIVED"
SAMPLE_RESULTED = "RESULTED"
SAMPLE_FROZEN = "FROZEN"

VERDICT_PASS = "PASS"
VERDICT_FAIL = "FAIL"

# ---- 清洁窗口状态 ----
WINDOW_OPEN = "OPEN"
WINDOW_CONCLUDED = "CONCLUDED"
WINDOW_FAILED = "FAILED"
WINDOW_FROZEN = "FROZEN"
WINDOW_SUPERSEDED = "SUPERSEDED"

# ---- 放行记录状态 ----
RELEASE_ACTIVE = "ACTIVE"
RELEASE_SUPERSEDED = "SUPERSEDED"

# ---- 隔离记录状态 ----
ISOLATION_OPEN = "OPEN"
ISOLATION_EXCEPTION = "EXCEPTION_APPROVED"
ISOLATION_REWORK = "RESOLVED_REWORK"

# ---- 风险处置状态 ----
DISPOSITION_OPEN = "OPEN"
DISPOSITION_NOTIFIED = "NOTIFIED"

# ---- 清洁结论 ----
CONCLUSION_PASS = "PASS"
CONCLUSION_INITIAL = "INITIAL"
CONCLUSION_EXCEPTION = "EXCEPTION"

SCHEMA = """
CREATE TABLE IF NOT EXISTS materials(
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL DEFAULT '',
  allergens TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS formula_versions(
  id TEXT PRIMARY KEY,
  materials TEXT NOT NULL,
  label_declarations TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS lines(
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL DEFAULT '',
  segments TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batches(
  id TEXT PRIMARY KEY,
  line_id TEXT NOT NULL,
  formula_version_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  status TEXT NOT NULL,
  window_id TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS plans(
  id TEXT PRIMARY KEY,
  line_id TEXT NOT NULL,
  from_batch_id TEXT NOT NULL,
  to_batch_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  status TEXT NOT NULL,
  risk_allergens TEXT NOT NULL,
  conclusion TEXT,
  created_at TEXT NOT NULL,
  concluded_at TEXT
);
CREATE TABLE IF NOT EXISTS plan_segments(
  id TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  segment_id TEXT NOT NULL,
  prev_materials TEXT NOT NULL,
  next_label_declarations TEXT NOT NULL,
  detection_limits TEXT NOT NULL,
  bypassed INTEGER NOT NULL DEFAULT 0,
  bypass_reason TEXT,
  bypassed_by TEXT,
  bypassed_at TEXT
);
CREATE TABLE IF NOT EXISTS cleaning_steps(
  id TEXT PRIMARY KEY,
  plan_segment_id TEXT NOT NULL,
  name TEXT NOT NULL,
  completed_by TEXT,
  completed_at TEXT
);
CREATE TABLE IF NOT EXISTS samples(
  sampling_no TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  plan_segment_id TEXT NOT NULL,
  allergen TEXT NOT NULL,
  detection_limit REAL,
  status TEXT NOT NULL,
  register_hash TEXT NOT NULL,
  result_hash TEXT,
  received_by TEXT,
  received_at TEXT,
  result_value REAL,
  result_by TEXT,
  result_at TEXT,
  verdict TEXT
);
CREATE TABLE IF NOT EXISTS windows(
  id TEXT PRIMARY KEY,
  line_id TEXT NOT NULL,
  plan_id TEXT,
  window_key TEXT NOT NULL,
  status TEXT NOT NULL,
  UNIQUE(line_id, window_key)
);
CREATE TABLE IF NOT EXISTS releases(
  id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL,
  plan_id TEXT,
  formula_version_id TEXT NOT NULL,
  label_snapshot TEXT NOT NULL,
  cleaning_conclusion TEXT NOT NULL,
  exception INTEGER NOT NULL DEFAULT 0,
  released_by TEXT NOT NULL,
  released_at TEXT NOT NULL,
  status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS isolations(
  id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  reason TEXT NOT NULL,
  sampling_no TEXT,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  exception_by TEXT,
  exception_reason TEXT,
  exception_at TEXT
);
CREATE TABLE IF NOT EXISTS dispositions(
  id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  sampling_no TEXT,
  inspection_snapshot TEXT NOT NULL,
  reason TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS disposition_notifications(
  id TEXT PRIMARY KEY,
  disposition_id TEXT NOT NULL,
  party TEXT NOT NULL,
  notified_by TEXT NOT NULL,
  notified_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events(
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  line_id TEXT,
  batch_id TEXT,
  payload TEXT NOT NULL,
  created_at TEXT NOT NULL
);
"""


class DomainError(Exception):
    """领域错误基类，携带 HTTP 状态码与稳定错误码。"""

    status = 400
    code = "domain_error"


class NotFound(DomainError):
    status = 404
    code = "not_found"


class Conflict(DomainError):
    status = 409
    code = "conflict"


class Validation(DomainError):
    status = 400
    code = "validation"


def _now():
    return datetime.now(timezone.utc).isoformat()


def _uid(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _dumps(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _hash_payload(payload):
    return hashlib.sha256(_dumps(payload).encode("utf-8")).hexdigest()


def _row(conn, sql, params=()):
    return conn.execute(sql, params).fetchone()


def _rows(conn, sql, params=()):
    return conn.execute(sql, params).fetchall()


class Store:
    """SQLite 存储，提供显式事务边界，支撑放行与重算的原子性。"""

    def __init__(self, path=":memory:"):
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.isolation_level = None  # 手动控制事务
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA)

    @contextmanager
    def tx(self):
        """写事务：任何一步失败都会整体回滚，不留半截状态。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    @contextmanager
    def read(self):
        with self._lock:
            yield self._conn

    def close(self):
        with self._lock:
            self._conn.close()


class ChangeoverService:
    """换线放行领域服务，所有写操作都在单事务内完成。"""

    def __init__(self, store):
        self.store = store

    # ------------------------------------------------------------------
    # 基础档案
    # ------------------------------------------------------------------
    def register_material(self, id, allergens, name=""):
        if not id:
            raise Validation("物料需要 id")
        if not isinstance(allergens, list):
            raise Validation("allergens 必须是列表")
        with self.store.tx() as conn:
            if _row(conn, "SELECT id FROM materials WHERE id=?", (id,)):
                raise Conflict(f"物料 {id} 已登记")
            conn.execute(
                "INSERT INTO materials(id, name, allergens) VALUES(?,?,?)",
                (id, name, _dumps(sorted(set(allergens)))),
            )
            return {"id": id, "name": name, "allergens": sorted(set(allergens))}

    def register_formula_version(self, id, materials, label_declarations):
        if not id:
            raise Validation("配方版本需要 id")
        if not isinstance(materials, list) or not materials:
            raise Validation("配方版本需要至少一个物料")
        if not isinstance(label_declarations, list):
            raise Validation("label_declarations 必须是列表")
        with self.store.tx() as conn:
            if _row(conn, "SELECT id FROM formula_versions WHERE id=?", (id,)):
                raise Conflict(f"配方版本 {id} 已登记")
            for material_id in materials:
                if not _row(conn, "SELECT id FROM materials WHERE id=?", (material_id,)):
                    raise Validation(f"配方版本引用了未登记的物料 {material_id}")
            conn.execute(
                "INSERT INTO formula_versions(id, materials, label_declarations) VALUES(?,?,?)",
                (id, _dumps(materials), _dumps(sorted(set(label_declarations)))),
            )
            return {
                "id": id,
                "materials": list(materials),
                "label_declarations": sorted(set(label_declarations)),
            }

    def register_line(self, id, segments, name=""):
        if not id:
            raise Validation("产线需要 id")
        if not isinstance(segments, list) or not segments:
            raise Validation("产线需要至少一个产线段")
        for segment in segments:
            if not segment.get("id"):
                raise Validation("产线段需要 id")
            if not isinstance(segment.get("steps", []), list):
                raise Validation(f"产线段 {segment.get('id')} 的 steps 必须是列表")
            if not isinstance(segment.get("detection_limits", {}), dict):
                raise Validation(f"产线段 {segment.get('id')} 的 detection_limits 必须是对象")
        with self.store.tx() as conn:
            if _row(conn, "SELECT id FROM lines WHERE id=?", (id,)):
                raise Conflict(f"产线 {id} 已登记")
            conn.execute(
                "INSERT INTO lines(id, name, segments) VALUES(?,?,?)",
                (id, name, _dumps(segments)),
            )
            return {"id": id, "name": name, "segments": segments}

    # ------------------------------------------------------------------
    # 生产顺序与换线计划
    # ------------------------------------------------------------------
    def append_sequence(self, line_id, batch_id, formula_version_id):
        """把批次追加到产线真实生产顺序末尾，并据此重算换线计划。"""
        with self.store.tx() as conn:
            self._get_line(conn, line_id)
            self._get_formula(conn, formula_version_id)
            if _row(conn, "SELECT id FROM batches WHERE id=?", (batch_id,)):
                raise Conflict(f"批次 {batch_id} 已存在")
            count = _row(conn, "SELECT COUNT(*) AS c FROM batches WHERE line_id=?", (line_id,))["c"]
            conn.execute(
                "INSERT INTO batches(id, line_id, formula_version_id, seq, status, created_at)"
                " VALUES(?,?,?,?,?,?)",
                (batch_id, line_id, formula_version_id, count + 1, BATCH_PLANNED, _now()),
            )
            rebuild = self._rebuild(conn, line_id, "SEQUENCE_APPEND", batch_id=batch_id)
            return {"batch": self._batch_view(self._get_batch(conn, batch_id)), "rebuild": rebuild}

    def insert_sequence(self, line_id, batch_id, formula_version_id, position):
        """计划外插单：把批次插入生产顺序指定位置并重新计算受影响范围。"""
        with self.store.tx() as conn:
            self._get_line(conn, line_id)
            self._get_formula(conn, formula_version_id)
            if _row(conn, "SELECT id FROM batches WHERE id=?", (batch_id,)):
                raise Conflict(f"批次 {batch_id} 已存在")
            count = _row(conn, "SELECT COUNT(*) AS c FROM batches WHERE line_id=?", (line_id,))["c"]
            if not isinstance(position, int) or isinstance(position, bool) or position < 0 or position > count:
                raise Validation(f"插单位置必须在 0 到 {count} 之间")
            conn.execute(
                "UPDATE batches SET seq=seq+1 WHERE line_id=? AND seq>?", (line_id, position)
            )
            conn.execute(
                "INSERT INTO batches(id, line_id, formula_version_id, seq, status, created_at)"
                " VALUES(?,?,?,?,?,?)",
                (batch_id, line_id, formula_version_id, position + 1, BATCH_PLANNED, _now()),
            )
            self._normalize_sequence(conn, line_id)
            rebuild = self._rebuild(conn, line_id, "UNSCHEDULED_INSERTION", batch_id=batch_id)
            return {"batch": self._batch_view(self._get_batch(conn, batch_id)), "rebuild": rebuild}

    def generate_plans(self, line_id):
        """从真实生产顺序（重新）生成换线计划，重复调用幂等。"""
        with self.store.tx() as conn:
            self._get_line(conn, line_id)
            rebuild = self._rebuild(conn, line_id, "PLAN_GENERATION")
            return {"line_id": line_id, "plans": self._list_plan_summaries(conn, line_id), **rebuild}

    def list_plans(self, line_id):
        with self.store.read() as conn:
            self._get_line(conn, line_id)
            return self._list_plan_summaries(conn, line_id)

    def get_plan(self, plan_id):
        with self.store.read() as conn:
            return self._plan_view(conn, self._get_plan(conn, plan_id))

    def get_window(self, window_id):
        with self.store.read() as conn:
            window = _row(conn, "SELECT * FROM windows WHERE id=?", (window_id,))
            if window is None:
                raise NotFound(f"清洁窗口 {window_id} 不存在")
            batches = _rows(
                conn,
                "SELECT * FROM batches WHERE window_id=? ORDER BY seq",
                (window_id,),
            )
            return {
                "id": window["id"],
                "line_id": window["line_id"],
                "plan_id": window["plan_id"],
                "status": window["status"],
                "batches": [self._batch_view(b) for b in batches],
            }

    # ------------------------------------------------------------------
    # 操作员拆洗步骤
    # ------------------------------------------------------------------
    def complete_step(self, plan_id, step_id, operator):
        if not operator:
            raise Validation("需要登记操作员")
        with self.store.tx() as conn:
            plan = self._get_plan(conn, plan_id)
            step = _row(
                conn,
                "SELECT s.* FROM cleaning_steps s"
                " JOIN plan_segments ps ON ps.id = s.plan_segment_id"
                " WHERE s.id=? AND ps.plan_id=?",
                (step_id, plan_id),
            )
            if step is None:
                raise NotFound(f"拆洗步骤 {step_id} 不在计划 {plan_id} 中")
            if plan["status"] != PLAN_OPEN:
                raise Conflict(f"换线计划状态为 {plan['status']}，不能登记步骤")
            if step["completed_by"] is not None:
                if step["completed_by"] == operator:
                    return self._plan_view(conn, plan)
                raise Conflict(f"步骤已由 {step['completed_by']} 完成")
            conn.execute(
                "UPDATE cleaning_steps SET completed_by=?, completed_at=? WHERE id=?",
                (operator, _now(), step_id),
            )
            return self._plan_view(conn, self._get_plan(conn, plan_id))

    # ------------------------------------------------------------------
    # 实验室样本签收与结果
    # ------------------------------------------------------------------
    def register_sample(self, plan_id, sampling_no, segment_id, allergen, detection_limit=None):
        """登记额外清洁验证样本；同编号同内容复用，异内容立即冻结。"""
        if not sampling_no or not allergen:
            raise Validation("样本需要 sampling_no 与 allergen")
        deferred = None
        with self.store.tx() as conn:
            plan = self._get_plan(conn, plan_id)
            if plan["status"] not in (PLAN_OPEN, PLAN_CONCLUDED, PLAN_FAILED, PLAN_FROZEN):
                raise Conflict(f"换线计划状态为 {plan['status']}，不能登记新样本")
            segment = _row(
                conn,
                "SELECT * FROM plan_segments WHERE plan_id=? AND segment_id=?",
                (plan_id, segment_id),
            )
            if segment is None:
                raise NotFound(f"产线段 {segment_id} 不在计划 {plan_id} 中")
            limits = json.loads(segment["detection_limits"])
            limit = detection_limit if detection_limit is not None else limits.get(allergen)
            if limit is None:
                raise Validation(f"产线段 {segment_id} 没有过敏原 {allergen} 的检测限")
            payload = {
                "sampling_no": sampling_no,
                "plan_id": plan_id,
                "segment_id": segment_id,
                "allergen": allergen,
                "detection_limit": limit,
            }
            existing = _row(conn, "SELECT * FROM samples WHERE sampling_no=?", (sampling_no,))
            if existing is not None:
                if existing["register_hash"] == _hash_payload(payload):
                    return self._sample_view(existing)  # 相同重传复用
                conn.execute(
                    "UPDATE samples SET status=? WHERE sampling_no=?",
                    (SAMPLE_FROZEN, sampling_no),
                )
                self._freeze_plan(conn, existing, "SAMPLE_REGISTRATION_CONFLICT", sampling_no)
                deferred = Conflict(f"采样编号 {sampling_no} 已存在且内容不一致，样本已冻结")
            else:
                self._insert_sample(
                    conn,
                    plan_id=plan_id,
                    plan_segment_id=segment["id"],
                    sampling_no=sampling_no,
                    allergen=allergen,
                    detection_limit=limit,
                    payload=payload,
                )
                return self._sample_view(
                    _row(conn, "SELECT * FROM samples WHERE sampling_no=?", (sampling_no,))
                )
        if deferred is not None:
            raise deferred

    def receive_sample(self, sampling_no, received_by):
        """实验室签收样本。"""
        if not received_by:
            raise Validation("需要登记签收人")
        with self.store.tx() as conn:
            sample = self._get_sample(conn, sampling_no)
            if sample["status"] == SAMPLE_FROZEN:
                raise Conflict(f"样本 {sampling_no} 已冻结")
            if sample["status"] == SAMPLE_RESULTED:
                raise Conflict(f"样本 {sampling_no} 已出具结果，不能重复签收")
            if sample["status"] == SAMPLE_RECEIVED:
                if sample["received_by"] == received_by:
                    return self._sample_view(sample)
                raise Conflict(f"样本 {sampling_no} 已由 {sample['received_by']} 签收")
            conn.execute(
                "UPDATE samples SET status=?, received_by=?, received_at=? WHERE sampling_no=?",
                (SAMPLE_RECEIVED, received_by, _now(), sampling_no),
            )
            return self._sample_view(self._get_sample(conn, sampling_no))

    def submit_result(self, sampling_no, result_value, result_by):
        """实验室出具结果；同编号同内容复用，异内容立即冻结。"""
        if not result_by:
            raise Validation("需要登记检测人")
        if not isinstance(result_value, (int, float)) or isinstance(result_value, bool):
            raise Validation("result_value 必须是数值")
        deferred = None
        with self.store.tx() as conn:
            sample = self._get_sample(conn, sampling_no)
            if sample["status"] == SAMPLE_FROZEN:
                raise Conflict(f"样本 {sampling_no} 已冻结")
            result_hash = _hash_payload(
                {"sampling_no": sampling_no, "result_value": result_value, "result_by": result_by}
            )
            if sample["status"] == SAMPLE_RESULTED:
                if sample["result_hash"] == result_hash:
                    return self._sample_view(sample)  # 相同重传复用结果
                # 异内容重传：冻结必须随事务提交落库，错误在提交后抛出
                conn.execute(
                    "UPDATE samples SET status=? WHERE sampling_no=?",
                    (SAMPLE_FROZEN, sampling_no),
                )
                self._freeze_plan(conn, sample, "SAMPLE_RESULT_CONFLICT", sampling_no)
                deferred = Conflict(f"采样编号 {sampling_no} 的重传内容不一致，样本已冻结")
            elif sample["status"] != SAMPLE_RECEIVED:
                raise Conflict(f"样本 {sampling_no} 尚未签收，不能出具结果")
            else:
                verdict = VERDICT_PASS if result_value <= sample["detection_limit"] else VERDICT_FAIL
                conn.execute(
                    "UPDATE samples SET status=?, result_hash=?, result_value=?, result_by=?,"
                    " result_at=?, verdict=? WHERE sampling_no=?",
                    (SAMPLE_RESULTED, result_hash, result_value, result_by, _now(), verdict,
                     sampling_no),
                )
                if verdict == VERDICT_FAIL:
                    self._fail_plan(
                        conn, self._get_sample(conn, sampling_no), "SAMPLE_FAILED", sampling_no
                    )
                return self._sample_view(self._get_sample(conn, sampling_no))
        if deferred is not None:
            raise deferred

    # ------------------------------------------------------------------
    # 批次生命周期与放行
    # ------------------------------------------------------------------
    def complete_batch(self, batch_id):
        """生产完成，进入待放行状态；若所在窗口已失效则立即隔离。"""
        with self.store.tx() as conn:
            batch = self._get_batch(conn, batch_id)
            if batch["status"] != BATCH_PLANNED:
                raise Conflict(f"批次 {batch_id} 状态为 {batch['status']}，不能登记完工")
            conn.execute("UPDATE batches SET status=? WHERE id=?", (BATCH_COMPLETED, batch_id))
            window = _row(conn, "SELECT * FROM windows WHERE id=?", (batch["window_id"],))
            affected = self._enforce_window(conn, window, "PRODUCTION_COMPLETED")
            self._event(
                conn, "BATCH_COMPLETED", line_id=batch["line_id"], batch_id=batch_id,
                payload={"affected_batches": affected},
            )
            return self._batch_view(self._get_batch(conn, batch_id))

    def release_batch(self, batch_id, released_by):
        """质量授权人放行：清洁结论、配方版本与标签快照在单事务中生效。"""
        if not released_by:
            raise Validation("放行需要质量授权人 released_by")
        with self.store.tx() as conn:
            batch = self._get_batch(conn, batch_id)
            active = _row(
                conn,
                "SELECT * FROM releases WHERE batch_id=? AND status=?",
                (batch_id, RELEASE_ACTIVE),
            )
            if active is not None:
                return self._release_view(active)  # 幂等：恢复后不重复放行
            if batch["status"] == BATCH_ISOLATED:
                raise Conflict(f"批次 {batch_id} 处于隔离状态，需例外批准")
            if batch["status"] != BATCH_COMPLETED:
                raise Conflict(f"批次 {batch_id} 状态为 {batch['status']}，不能放行")
            window = _row(conn, "SELECT * FROM windows WHERE id=?", (batch["window_id"],))
            plan = (
                _row(conn, "SELECT * FROM plans WHERE id=?", (window["plan_id"],))
                if window and window["plan_id"]
                else None
            )
            if plan is None:
                conclusion = CONCLUSION_INITIAL
                plan_id = None
            elif plan["status"] == PLAN_CONCLUDED:
                conclusion = plan["conclusion"]
                plan_id = plan["id"]
            elif plan["status"] == PLAN_OPEN:
                gaps = self._coverage_gaps(conn, plan)
                if gaps:
                    raise Conflict("换线覆盖不完整: " + "; ".join(gaps))
                concluded_at = _now()
                conn.execute(
                    "UPDATE plans SET status=?, conclusion=?, concluded_at=? WHERE id=?",
                    (PLAN_CONCLUDED, CONCLUSION_PASS, concluded_at, plan_id := plan["id"]),
                )
                conn.execute(
                    "UPDATE windows SET status=? WHERE id=?", (WINDOW_CONCLUDED, window["id"])
                )
                conclusion = CONCLUSION_PASS
            else:
                raise Conflict(f"换线计划状态为 {plan['status']}，不允许放行")
            formula = self._get_formula(conn, batch["formula_version_id"])
            release_id = _uid("rel")
            conn.execute(
                "INSERT INTO releases(id, batch_id, plan_id, formula_version_id, label_snapshot,"
                " cleaning_conclusion, exception, released_by, released_at, status)"
                " VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    release_id, batch_id, plan_id, formula["id"],
                    formula["label_declarations"], conclusion, 0, released_by, _now(),
                    RELEASE_ACTIVE,
                ),
            )
            conn.execute("UPDATE batches SET status=? WHERE id=?", (BATCH_RELEASED, batch_id))
            self._event(
                conn, "BATCH_RELEASED", line_id=batch["line_id"], batch_id=batch_id,
                payload={
                    "release_id": release_id, "plan_id": plan_id,
                    "cleaning_conclusion": conclusion, "released_by": released_by,
                    "affected_batches": [batch_id],
                },
            )
            return self._release_view(_row(conn, "SELECT * FROM releases WHERE id=?", (release_id,)))

    def ship_batch(self, batch_id, shipped_by=""):
        with self.store.tx() as conn:
            batch = self._get_batch(conn, batch_id)
            if batch["status"] != BATCH_RELEASED:
                raise Conflict(f"批次 {batch_id} 状态为 {batch['status']}，不能出厂")
            conn.execute("UPDATE batches SET status=? WHERE id=?", (BATCH_SHIPPED, batch_id))
            self._event(
                conn, "BATCH_SHIPPED", line_id=batch["line_id"], batch_id=batch_id,
                payload={"shipped_by": shipped_by, "affected_batches": [batch_id]},
            )
            return self._batch_view(self._get_batch(conn, batch_id))

    def rework_batch(self, batch_id, reason=""):
        """返工：批次重回生产顺序末尾，原放行作废，并重算受影响范围。"""
        with self.store.tx() as conn:
            batch = self._get_batch(conn, batch_id)
            if batch["status"] == BATCH_SHIPPED:
                raise Conflict(f"批次 {batch_id} 已出厂，不能返工，请通过风险处置跟踪")
            conn.execute(
                "UPDATE releases SET status=? WHERE batch_id=? AND status=?",
                (RELEASE_SUPERSEDED, batch_id, RELEASE_ACTIVE),
            )
            conn.execute(
                "UPDATE isolations SET status=? WHERE batch_id=? AND status=?",
                (ISOLATION_REWORK, batch_id, ISOLATION_OPEN),
            )
            max_seq = _row(
                conn, "SELECT COALESCE(MAX(seq),0) AS m FROM batches WHERE line_id=?",
                (batch["line_id"],),
            )["m"]
            conn.execute(
                "UPDATE batches SET seq=?, status=? WHERE id=?",
                (max_seq + 1, BATCH_PLANNED, batch_id),
            )
            self._normalize_sequence(conn, batch["line_id"])
            rebuild = self._rebuild(conn, batch["line_id"], "REWORK", batch_id=batch_id)
            return {"batch": self._batch_view(self._get_batch(conn, batch_id)), "rebuild": rebuild}

    def bypass_segment(self, plan_id, segment_id, reason="", by=""):
        """设备旁路：使计划失效并重新计算受影响范围。"""
        if not by:
            raise Validation("需要登记旁路操作人 by")
        with self.store.tx() as conn:
            plan = self._get_plan(conn, plan_id)
            if plan["status"] not in (PLAN_OPEN, PLAN_CONCLUDED):
                raise Conflict(f"换线计划状态为 {plan['status']}，不能登记旁路")
            segment = _row(
                conn,
                "SELECT * FROM plan_segments WHERE plan_id=? AND segment_id=?",
                (plan_id, segment_id),
            )
            if segment is None:
                raise NotFound(f"产线段 {segment_id} 不在计划 {plan_id} 中")
            if segment["bypassed"]:
                raise Conflict(f"产线段 {segment_id} 已登记旁路")
            conn.execute(
                "UPDATE plan_segments SET bypassed=1, bypass_reason=?, bypassed_by=?, bypassed_at=?"
                " WHERE id=?",
                (reason, by, _now(), segment["id"]),
            )
            conn.execute("UPDATE plans SET status=? WHERE id=?", (PLAN_INVALID, plan_id))
            conn.execute("UPDATE windows SET status=? WHERE id=?", (WINDOW_FAILED, plan["window_id"]))
            window = _row(conn, "SELECT * FROM windows WHERE id=?", (plan["window_id"],))
            affected = self._enforce_window(conn, window, "EQUIPMENT_BYPASS")
            self._event(
                conn, "EQUIPMENT_BYPASS", line_id=plan["line_id"],
                payload={
                    "plan_id": plan_id, "segment_id": segment_id, "reason": reason, "by": by,
                    "affected_batches": affected,
                },
            )
            return {"plan_id": plan_id, "status": PLAN_INVALID, "affected_batches": affected}

    # ------------------------------------------------------------------
    # 隔离、例外批准与风险处置
    # ------------------------------------------------------------------
    def approve_exception(self, isolation_id, approved_by="", reason=""):
        """例外批准：质量授权人对隔离批次给出带理由的放行。"""
        if not approved_by or not reason:
            raise Validation("例外批准需要 approved_by 与 reason")
        with self.store.tx() as conn:
            isolation = _row(conn, "SELECT * FROM isolations WHERE id=?", (isolation_id,))
            if isolation is None:
                raise NotFound(f"隔离记录 {isolation_id} 不存在")
            if isolation["status"] != ISOLATION_OPEN:
                raise Conflict(f"隔离记录状态为 {isolation['status']}，不能重复批准")
            batch = self._get_batch(conn, isolation["batch_id"])
            if batch["status"] != BATCH_ISOLATED:
                raise Conflict(f"批次 {batch['id']} 当前不在隔离状态")
            window = _row(conn, "SELECT * FROM windows WHERE id=?", (isolation["window_id"],))
            formula = self._get_formula(conn, batch["formula_version_id"])
            conn.execute(
                "UPDATE isolations SET status=?, exception_by=?, exception_reason=?, exception_at=?"
                " WHERE id=?",
                (ISOLATION_EXCEPTION, approved_by, reason, _now(), isolation_id),
            )
            release_id = _uid("rel")
            conn.execute(
                "INSERT INTO releases(id, batch_id, plan_id, formula_version_id, label_snapshot,"
                " cleaning_conclusion, exception, released_by, released_at, status)"
                " VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    release_id, batch["id"], window["plan_id"] if window else None, formula["id"],
                    formula["label_declarations"], CONCLUSION_EXCEPTION, 1, approved_by, _now(),
                    RELEASE_ACTIVE,
                ),
            )
            conn.execute("UPDATE batches SET status=? WHERE id=?", (BATCH_RELEASED, batch["id"]))
            self._event(
                conn, "EXCEPTION_APPROVED", line_id=batch["line_id"], batch_id=batch["id"],
                payload={
                    "isolation_id": isolation_id, "release_id": release_id,
                    "approved_by": approved_by, "reason": reason,
                    "affected_batches": [batch["id"]],
                },
            )
            return self._release_view(_row(conn, "SELECT * FROM releases WHERE id=?", (release_id,)))

    def add_notification(self, disposition_id, party="", notified_by=""):
        """登记风险处置的通知对象，形成可追踪的通知范围。"""
        if not party:
            raise Validation("需要登记通知对象 party")
        with self.store.tx() as conn:
            disposition = _row(conn, "SELECT * FROM dispositions WHERE id=?", (disposition_id,))
            if disposition is None:
                raise NotFound(f"风险处置 {disposition_id} 不存在")
            conn.execute(
                "INSERT INTO disposition_notifications(id, disposition_id, party, notified_by,"
                " notified_at) VALUES(?,?,?,?,?)",
                (_uid("ntf"), disposition_id, party, notified_by, _now()),
            )
            conn.execute(
                "UPDATE dispositions SET status=? WHERE id=?", (DISPOSITION_NOTIFIED, disposition_id)
            )
            self._event(
                conn, "NOTIFICATION_SENT", batch_id=disposition["batch_id"],
                payload={
                    "disposition_id": disposition_id, "party": party, "notified_by": notified_by,
                    "affected_batches": [disposition["batch_id"]],
                },
            )
            return self._disposition_view(
                conn, _row(conn, "SELECT * FROM dispositions WHERE id=?", (disposition_id,))
            )

    # ------------------------------------------------------------------
    # 追溯
    # ------------------------------------------------------------------
    def trace_batch(self, batch_id):
        """从成品反查换线责任、样本证据、例外批准及通知范围。"""
        with self.store.read() as conn:
            batch = self._get_batch(conn, batch_id)
            formula = self._get_formula(conn, batch["formula_version_id"])
            materials = []
            for material_id in json.loads(formula["materials"]):
                material = _row(conn, "SELECT * FROM materials WHERE id=?", (material_id,))
                materials.append(
                    {
                        "id": material["id"],
                        "name": material["name"],
                        "allergens": json.loads(material["allergens"]),
                    }
                )
            window = _row(conn, "SELECT * FROM windows WHERE id=?", (batch["window_id"],))
            plan = None
            if window and window["plan_id"]:
                plan = self._plan_view(
                    conn, _row(conn, "SELECT * FROM plans WHERE id=?", (window["plan_id"],))
                )
            releases = [
                self._release_view(r)
                for r in _rows(
                    conn, "SELECT * FROM releases WHERE batch_id=? ORDER BY rowid", (batch_id,)
                )
            ]
            isolations = [
                self._isolation_view(r)
                for r in _rows(
                    conn, "SELECT * FROM isolations WHERE batch_id=? ORDER BY rowid", (batch_id,)
                )
            ]
            dispositions = [
                self._disposition_view(conn, r)
                for r in _rows(
                    conn, "SELECT * FROM dispositions WHERE batch_id=? ORDER BY rowid", (batch_id,)
                )
            ]
            events = []
            for event in _rows(
                conn,
                "SELECT * FROM events WHERE line_id=? OR batch_id=? ORDER BY rowid",
                (batch["line_id"], batch_id),
            ):
                payload = json.loads(event["payload"])
                if event["batch_id"] == batch_id or batch_id in payload.get("affected_batches", []):
                    events.append(
                        {
                            "id": event["id"],
                            "type": event["type"],
                            "payload": payload,
                            "created_at": event["created_at"],
                        }
                    )
            return {
                "batch": self._batch_view(batch),
                "formula_version": {
                    "id": formula["id"],
                    "materials": materials,
                    "label_declarations": json.loads(formula["label_declarations"]),
                },
                "window": (
                    {"id": window["id"], "status": window["status"], "plan_id": window["plan_id"]}
                    if window
                    else None
                ),
                "plan": plan,
                "releases": releases,
                "isolations": isolations,
                "dispositions": dispositions,
                "events": events,
            }

    # ------------------------------------------------------------------
    # 内部：顺序重算与窗口完整性
    # ------------------------------------------------------------------
    def _rebuild(self, conn, line_id, trigger, batch_id=None):
        """按真实生产顺序重算换线计划、清洁窗口与受影响批次范围。"""
        line = self._get_line(conn, line_id)
        batches = _rows(conn, "SELECT * FROM batches WHERE line_id=? ORDER BY seq, rowid", (line_id,))
        desired = []  # [(plan_key|None, [batch_id, ...])]
        for index, batch in enumerate(batches):
            if index == 0:
                desired.append((None, [batch["id"]]))
                continue
            prev = batches[index - 1]
            risk = self._risk_allergens(conn, prev["formula_version_id"], batch["formula_version_id"])
            if risk:
                desired.append(((prev["id"], batch["id"]), [batch["id"]]))
            else:
                desired[-1][1].append(batch["id"])
        existing = {}
        for plan in _rows(
            conn, "SELECT * FROM plans WHERE line_id=? AND status!=?", (line_id, PLAN_SUPERSEDED)
        ):
            existing[(plan["from_batch_id"], plan["to_batch_id"])] = plan
        desired_keys = {key for key, _ in desired if key is not None}
        superseded = []
        affected = []
        for key, plan in existing.items():
            if key not in desired_keys:
                conn.execute("UPDATE plans SET status=? WHERE id=?", (PLAN_SUPERSEDED, plan["id"]))
                conn.execute(
                    "UPDATE windows SET status=? WHERE id=?", (WINDOW_SUPERSEDED, plan["window_id"])
                )
                superseded.append(plan["id"])
                # 窗口失效先于批次迁移处理，确保已放行/已出厂批次被纳入受影响范围
                window = _row(conn, "SELECT * FROM windows WHERE id=?", (plan["window_id"],))
                affected.extend(self._enforce_window(conn, window, trigger))
        initial_window_id = self._ensure_window(conn, line_id, None, "INITIAL")
        new_plans = []
        batch_window = {}
        for key, batch_ids in desired:
            if key is None:
                window_id = initial_window_id
            else:
                plan = existing.get(key)
                if plan is None:
                    plan_id = self._create_plan(conn, line, key[0], key[1])
                    new_plans.append(plan_id)
                    plan = _row(conn, "SELECT * FROM plans WHERE id=?", (plan_id,))
                window_id = plan["window_id"]
            for bid in batch_ids:
                batch_window[bid] = window_id
        for bid, wid in batch_window.items():
            conn.execute("UPDATE batches SET window_id=? WHERE id=?", (wid, bid))
        for window in _rows(conn, "SELECT * FROM windows WHERE line_id=?", (line_id,)):
            affected.extend(self._enforce_window(conn, window, trigger))
        affected = sorted(set(affected))
        self._event(
            conn, trigger, line_id=line_id, batch_id=batch_id,
            payload={
                "new_plans": new_plans,
                "superseded_plans": superseded,
                "affected_batches": affected,
            },
        )
        return {
            "new_plans": new_plans,
            "superseded_plans": superseded,
            "affected_batches": affected,
        }

    def _create_plan(self, conn, line, from_batch_id, to_batch_id):
        """按计划生成时的真实顺序快照，登记五个要素到每个产线段。"""
        from_batch = self._get_batch(conn, from_batch_id)
        to_batch = self._get_batch(conn, to_batch_id)
        prev_formula = self._get_formula(conn, from_batch["formula_version_id"])
        next_formula = self._get_formula(conn, to_batch["formula_version_id"])
        prev_materials = []
        for material_id in json.loads(prev_formula["materials"]):
            material = _row(conn, "SELECT * FROM materials WHERE id=?", (material_id,))
            prev_materials.append(
                {
                    "id": material["id"],
                    "name": material["name"],
                    "allergens": json.loads(material["allergens"]),
                }
            )
        next_label = json.loads(next_formula["label_declarations"])
        risk = sorted(self._formula_allergens(conn, prev_formula["id"]) - set(next_label))
        plan_id = _uid("plan")
        window_id = _uid("win")
        conn.execute(
            "INSERT INTO windows(id, line_id, plan_id, window_key, status) VALUES(?,?,?,?,?)",
            (window_id, line["id"], plan_id, plan_id, WINDOW_OPEN),
        )
        conn.execute(
            "INSERT INTO plans(id, line_id, from_batch_id, to_batch_id, window_id, status,"
            " risk_allergens, created_at) VALUES(?,?,?,?,?,?,?,?)",
            (plan_id, line["id"], from_batch_id, to_batch_id, window_id, PLAN_OPEN,
             _dumps(risk), _now()),
        )
        for segment in json.loads(line["segments"]):
            limits = segment.get("detection_limits") or {}
            missing = [a for a in risk if a not in limits]
            if missing:
                raise Validation(
                    f"产线段 {segment['id']} 缺少过敏原 {','.join(missing)} 的检测限，"
                    "无法生成换线计划"
                )
            plan_segment_id = _uid("ps")
            conn.execute(
                "INSERT INTO plan_segments(id, plan_id, segment_id, prev_materials,"
                " next_label_declarations, detection_limits) VALUES(?,?,?,?,?,?)",
                (plan_segment_id, plan_id, segment["id"], _dumps(prev_materials),
                 _dumps(next_label), _dumps(limits)),
            )
            for step_name in segment.get("steps") or []:
                conn.execute(
                    "INSERT INTO cleaning_steps(id, plan_segment_id, name) VALUES(?,?,?)",
                    (_uid("step"), plan_segment_id, step_name),
                )
            for allergen in risk:
                sampling_no = f"{plan_id}:{segment['id']}:{allergen}"
                self._insert_sample(
                    conn,
                    plan_id=plan_id,
                    plan_segment_id=plan_segment_id,
                    sampling_no=sampling_no,
                    allergen=allergen,
                    detection_limit=limits[allergen],
                    payload={
                        "sampling_no": sampling_no,
                        "plan_id": plan_id,
                        "segment_id": segment["id"],
                        "allergen": allergen,
                        "detection_limit": limits[allergen],
                    },
                )
        return plan_id

    def _enforce_window(self, conn, window, reason, sampling_no=None):
        """窗口失效时收紧范围：未出厂批次隔离，已出厂批次生成风险处置。"""
        affected = []
        if window is None or not window["plan_id"]:
            return affected
        plan = _row(conn, "SELECT * FROM plans WHERE id=?", (window["plan_id"],))
        if plan is None or plan["status"] not in PLAN_BAD_STATES:
            return affected
        for batch in _rows(conn, "SELECT * FROM batches WHERE window_id=?", (window["id"],)):
            if batch["status"] == BATCH_SHIPPED:
                duplicate = _row(
                    conn,
                    "SELECT id FROM dispositions WHERE batch_id=? AND window_id=?",
                    (batch["id"], window["id"]),
                )
                if duplicate is None:
                    disposition_id = _uid("disp")
                    snapshot = self._inspection_snapshot(conn, plan, sampling_no)
                    conn.execute(
                        "INSERT INTO dispositions(id, batch_id, window_id, sampling_no,"
                        " inspection_snapshot, reason, status, created_at)"
                        " VALUES(?,?,?,?,?,?,?,?)",
                        (disposition_id, batch["id"], window["id"], sampling_no,
                         _dumps(snapshot), reason, DISPOSITION_OPEN, _now()),
                    )
                    self._event(
                        conn, "DISPOSITION_CREATED", line_id=batch["line_id"], batch_id=batch["id"],
                        payload={
                            "disposition_id": disposition_id, "window_id": window["id"],
                            "reason": reason, "affected_batches": [batch["id"]],
                        },
                    )
                    affected.append(batch["id"])
            elif batch["status"] in (BATCH_COMPLETED, BATCH_RELEASED):
                if batch["status"] == BATCH_RELEASED:
                    conn.execute(
                        "UPDATE releases SET status=? WHERE batch_id=? AND status=?",
                        (RELEASE_SUPERSEDED, batch["id"], RELEASE_ACTIVE),
                    )
                open_isolation = _row(
                    conn,
                    "SELECT id FROM isolations WHERE batch_id=? AND status=?",
                    (batch["id"], ISOLATION_OPEN),
                )
                if open_isolation is None:
                    isolation_id = _uid("iso")
                    conn.execute(
                        "INSERT INTO isolations(id, batch_id, window_id, reason, sampling_no,"
                        " status, created_at) VALUES(?,?,?,?,?,?,?)",
                        (isolation_id, batch["id"], window["id"], reason, sampling_no,
                         ISOLATION_OPEN, _now()),
                    )
                    conn.execute(
                        "UPDATE batches SET status=? WHERE id=?", (BATCH_ISOLATED, batch["id"])
                    )
                    self._event(
                        conn, "BATCH_ISOLATED", line_id=batch["line_id"], batch_id=batch["id"],
                        payload={
                            "isolation_id": isolation_id, "window_id": window["id"],
                            "reason": reason, "sampling_no": sampling_no,
                            "affected_batches": [batch["id"]],
                        },
                    )
                    affected.append(batch["id"])
        return affected

    def _inspection_snapshot(self, conn, plan, sampling_no):
        """风险处置引用原检验快照，原检验记录本身不被改写。"""
        if sampling_no:
            sample = _row(conn, "SELECT * FROM samples WHERE sampling_no=?", (sampling_no,))
            if sample is not None:
                return {"type": "sample", **self._sample_view(sample)}
        return {
            "type": "plan",
            "plan_id": plan["id"],
            "plan_status": plan["status"],
            "from_batch_id": plan["from_batch_id"],
            "to_batch_id": plan["to_batch_id"],
        }

    def _fail_plan(self, conn, sample, reason, sampling_no):
        plan = _row(conn, "SELECT * FROM plans WHERE id=?", (sample["plan_id"],))
        status = plan["status"]
        if status in (PLAN_OPEN, PLAN_CONCLUDED):
            conn.execute("UPDATE plans SET status=? WHERE id=?", (PLAN_FAILED, plan["id"]))
            conn.execute(
                "UPDATE windows SET status=? WHERE id=?", (WINDOW_FAILED, plan["window_id"])
            )
        elif status != PLAN_FAILED:
            return  # 已冻结/失效/被取代的计划不再扩散
        # 已失败的计划仍允许迟到证据登记，并再次收紧窗口范围
        window = _row(conn, "SELECT * FROM windows WHERE id=?", (plan["window_id"],))
        affected = self._enforce_window(conn, window, reason, sampling_no)
        self._event(
            conn, "SAMPLE_FAILED", line_id=plan["line_id"],
            payload={
                "sampling_no": sampling_no, "plan_id": plan["id"], "reason": reason,
                "affected_batches": affected,
            },
        )

    def _freeze_plan(self, conn, sample, reason, sampling_no):
        plan = _row(conn, "SELECT * FROM plans WHERE id=?", (sample["plan_id"],))
        status = plan["status"]
        if status in (PLAN_OPEN, PLAN_CONCLUDED):
            conn.execute("UPDATE plans SET status=? WHERE id=?", (PLAN_FROZEN, plan["id"]))
            conn.execute(
                "UPDATE windows SET status=? WHERE id=?", (WINDOW_FROZEN, plan["window_id"])
            )
        elif status != PLAN_FROZEN:
            return
        window = _row(conn, "SELECT * FROM windows WHERE id=?", (plan["window_id"],))
        affected = self._enforce_window(conn, window, reason, sampling_no)
        self._event(
            conn, "SAMPLE_FROZEN", line_id=plan["line_id"],
            payload={
                "sampling_no": sampling_no, "plan_id": plan["id"], "reason": reason,
                "affected_batches": affected,
            },
        )

    def _coverage_gaps(self, conn, plan):
        """覆盖完整性：每个产线段步骤完成且每个风险过敏原都有合格结果。"""
        gaps = []
        risk = json.loads(plan["risk_allergens"])
        segments = _rows(
            conn, "SELECT * FROM plan_segments WHERE plan_id=? ORDER BY segment_id", (plan["id"],)
        )
        for segment in segments:
            if segment["bypassed"]:
                gaps.append(f"产线段 {segment['segment_id']} 已被旁路")
                continue
            pending = _rows(
                conn,
                "SELECT id FROM cleaning_steps WHERE plan_segment_id=? AND completed_by IS NULL",
                (segment["id"],),
            )
            if pending:
                gaps.append(f"产线段 {segment['segment_id']} 还有 {len(pending)} 个拆洗步骤未完成")
            for allergen in risk:
                samples = _rows(
                    conn,
                    "SELECT * FROM samples WHERE plan_segment_id=? AND allergen=?",
                    (segment["id"], allergen),
                )
                if not samples:
                    gaps.append(f"产线段 {segment['segment_id']} 缺少过敏原 {allergen} 的清洁验证样本")
                    continue
                if any(s["status"] == SAMPLE_FROZEN for s in samples):
                    gaps.append(f"产线段 {segment['segment_id']} 过敏原 {allergen} 存在被冻结样本")
                if any(
                    s["status"] == SAMPLE_RESULTED and s["verdict"] == VERDICT_FAIL for s in samples
                ):
                    gaps.append(f"产线段 {segment['segment_id']} 过敏原 {allergen} 检测超出检测限")
                if not any(
                    s["status"] == SAMPLE_RESULTED and s["verdict"] == VERDICT_PASS for s in samples
                ):
                    gaps.append(f"产线段 {segment['segment_id']} 过敏原 {allergen} 没有合格检测结果")
        return gaps

    # ------------------------------------------------------------------
    # 内部：数据访问与视图
    # ------------------------------------------------------------------
    def _get_line(self, conn, line_id):
        line = _row(conn, "SELECT * FROM lines WHERE id=?", (line_id,))
        if line is None:
            raise NotFound(f"产线 {line_id} 不存在")
        return line

    def _get_formula(self, conn, formula_version_id):
        formula = _row(conn, "SELECT * FROM formula_versions WHERE id=?", (formula_version_id,))
        if formula is None:
            raise NotFound(f"配方版本 {formula_version_id} 不存在")
        return formula

    def _get_batch(self, conn, batch_id):
        batch = _row(conn, "SELECT * FROM batches WHERE id=?", (batch_id,))
        if batch is None:
            raise NotFound(f"批次 {batch_id} 不存在")
        return batch

    def _get_plan(self, conn, plan_id):
        plan = _row(conn, "SELECT * FROM plans WHERE id=?", (plan_id,))
        if plan is None:
            raise NotFound(f"换线计划 {plan_id} 不存在")
        return plan

    def _get_sample(self, conn, sampling_no):
        sample = _row(conn, "SELECT * FROM samples WHERE sampling_no=?", (sampling_no,))
        if sample is None:
            raise NotFound(f"样本 {sampling_no} 不存在")
        return sample

    def _formula_allergens(self, conn, formula_version_id):
        formula = self._get_formula(conn, formula_version_id)
        allergens = set()
        for material_id in json.loads(formula["materials"]):
            material = _row(conn, "SELECT * FROM materials WHERE id=?", (material_id,))
            if material is None:
                raise Validation(f"配方 {formula_version_id} 引用了未登记的物料 {material_id}")
            allergens.update(json.loads(material["allergens"]))
        return allergens

    def _risk_allergens(self, conn, from_formula_id, to_formula_id):
        """上一批可能带入且下一批标签未声明的过敏原。"""
        declared = set(json.loads(self._get_formula(conn, to_formula_id)["label_declarations"]))
        return sorted(self._formula_allergens(conn, from_formula_id) - declared)

    def _ensure_window(self, conn, line_id, plan_id, window_key):
        window = _row(
            conn, "SELECT * FROM windows WHERE line_id=? AND window_key=?", (line_id, window_key)
        )
        if window is not None:
            return window["id"]
        window_id = _uid("win")
        conn.execute(
            "INSERT INTO windows(id, line_id, plan_id, window_key, status) VALUES(?,?,?,?,?)",
            (window_id, line_id, plan_id, window_key, WINDOW_OPEN),
        )
        return window_id

    def _normalize_sequence(self, conn, line_id):
        batches = _rows(
            conn, "SELECT id FROM batches WHERE line_id=? ORDER BY seq, rowid", (line_id,)
        )
        for index, batch in enumerate(batches, start=1):
            conn.execute("UPDATE batches SET seq=? WHERE id=?", (index, batch["id"]))

    def _insert_sample(self, conn, *, plan_id, plan_segment_id, sampling_no, allergen,
                       detection_limit, payload):
        conn.execute(
            "INSERT INTO samples(sampling_no, plan_id, plan_segment_id, allergen, detection_limit,"
            " status, register_hash) VALUES(?,?,?,?,?,?,?)",
            (sampling_no, plan_id, plan_segment_id, allergen, detection_limit, SAMPLE_REGISTERED,
             _hash_payload(payload)),
        )

    def _event(self, conn, event_type, line_id=None, batch_id=None, payload=None):
        conn.execute(
            "INSERT INTO events(id, type, line_id, batch_id, payload, created_at)"
            " VALUES(?,?,?,?,?,?)",
            (_uid("evt"), event_type, line_id, batch_id, _dumps(payload or {}), _now()),
        )

    def _list_plan_summaries(self, conn, line_id):
        return [
            {
                "id": plan["id"],
                "from_batch_id": plan["from_batch_id"],
                "to_batch_id": plan["to_batch_id"],
                "window_id": plan["window_id"],
                "status": plan["status"],
                "risk_allergens": json.loads(plan["risk_allergens"]),
                "conclusion": plan["conclusion"],
            }
            for plan in _rows(
                conn, "SELECT * FROM plans WHERE line_id=? ORDER BY rowid", (line_id,)
            )
        ]

    def _batch_view(self, batch):
        return {
            "id": batch["id"],
            "line_id": batch["line_id"],
            "formula_version_id": batch["formula_version_id"],
            "sequence": batch["seq"],
            "status": batch["status"],
            "window_id": batch["window_id"],
            "created_at": batch["created_at"],
        }

    def _plan_view(self, conn, plan):
        segments = []
        for segment in _rows(
            conn, "SELECT * FROM plan_segments WHERE plan_id=? ORDER BY segment_id", (plan["id"],)
        ):
            steps = [
                {
                    "id": step["id"],
                    "name": step["name"],
                    "completed_by": step["completed_by"],
                    "completed_at": step["completed_at"],
                }
                for step in _rows(
                    conn,
                    "SELECT * FROM cleaning_steps WHERE plan_segment_id=? ORDER BY rowid",
                    (segment["id"],),
                )
            ]
            samples = [
                self._sample_view(sample)
                for sample in _rows(
                    conn,
                    "SELECT * FROM samples WHERE plan_segment_id=? ORDER BY sampling_no",
                    (segment["id"],),
                )
            ]
            segments.append(
                {
                    "id": segment["id"],
                    "segment_id": segment["segment_id"],
                    "prev_materials": json.loads(segment["prev_materials"]),
                    "next_label_declarations": json.loads(segment["next_label_declarations"]),
                    "detection_limits": json.loads(segment["detection_limits"]),
                    "bypassed": bool(segment["bypassed"]),
                    "bypass_reason": segment["bypass_reason"],
                    "bypassed_by": segment["bypassed_by"],
                    "bypassed_at": segment["bypassed_at"],
                    "steps": steps,
                    "samples": samples,
                }
            )
        return {
            "id": plan["id"],
            "line_id": plan["line_id"],
            "from_batch_id": plan["from_batch_id"],
            "to_batch_id": plan["to_batch_id"],
            "window_id": plan["window_id"],
            "status": plan["status"],
            "risk_allergens": json.loads(plan["risk_allergens"]),
            "conclusion": plan["conclusion"],
            "created_at": plan["created_at"],
            "concluded_at": plan["concluded_at"],
            "segments": segments,
        }

    def _sample_view(self, sample):
        return {
            "sampling_no": sample["sampling_no"],
            "plan_id": sample["plan_id"],
            "allergen": sample["allergen"],
            "detection_limit": sample["detection_limit"],
            "status": sample["status"],
            "received_by": sample["received_by"],
            "received_at": sample["received_at"],
            "result_value": sample["result_value"],
            "result_by": sample["result_by"],
            "result_at": sample["result_at"],
            "verdict": sample["verdict"],
        }

    def _release_view(self, release):
        return {
            "id": release["id"],
            "batch_id": release["batch_id"],
            "plan_id": release["plan_id"],
            "formula_version_id": release["formula_version_id"],
            "label_snapshot": json.loads(release["label_snapshot"]),
            "cleaning_conclusion": release["cleaning_conclusion"],
            "exception": bool(release["exception"]),
            "released_by": release["released_by"],
            "released_at": release["released_at"],
            "status": release["status"],
        }

    def _isolation_view(self, isolation):
        return {
            "id": isolation["id"],
            "batch_id": isolation["batch_id"],
            "window_id": isolation["window_id"],
            "reason": isolation["reason"],
            "sampling_no": isolation["sampling_no"],
            "status": isolation["status"],
            "created_at": isolation["created_at"],
            "exception_by": isolation["exception_by"],
            "exception_reason": isolation["exception_reason"],
            "exception_at": isolation["exception_at"],
        }

    def _disposition_view(self, conn, disposition):
        notifications = [
            {
                "id": note["id"],
                "party": note["party"],
                "notified_by": note["notified_by"],
                "notified_at": note["notified_at"],
            }
            for note in _rows(
                conn,
                "SELECT * FROM disposition_notifications WHERE disposition_id=? ORDER BY rowid",
                (disposition["id"],),
            )
        ]
        return {
            "id": disposition["id"],
            "batch_id": disposition["batch_id"],
            "window_id": disposition["window_id"],
            "sampling_no": disposition["sampling_no"],
            "inspection_snapshot": json.loads(disposition["inspection_snapshot"]),
            "reason": disposition["reason"],
            "status": disposition["status"],
            "created_at": disposition["created_at"],
            "notifications": notifications,
        }
