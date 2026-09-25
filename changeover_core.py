"""共享产线过敏原换线放行领域核心。

中试工厂在同一条结构化产线上轮换豌豆、蚕豆和含麸质物料。本模块把
换线计划、清洁执行、实验室结果、质量放行与追溯收敛到同一个事务化
存储上,保证:

* 换线计划只从真实生产顺序生成,按产线段登记上一批物料、拆洗步骤、
  清洁验证样本、检测限和下一批标签声明;
* 质量授权人只对覆盖完整且检测满足阈值的批次放行,清洁结论、配方
  版本和标签快照在同一事务中生效,服务恢复后不会重复放行;
* 抽样失败隔离同一清洁窗口内尚未出厂的批次,已出厂部分生成可追踪
  风险处置,原检验记录不被改写;
* 返工、设备旁路、计划外插单会重新计算受影响范围;
* 同一采样编号的相同重传复用结果,异内容立即冻结。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

KNOWN_ALLERGENS = ("pea", "fava", "gluten")

# 批次状态
BATCH_PENDING = "PENDING"
BATCH_ISOLATED = "ISOLATED"
BATCH_RELEASED = "RELEASED"

# 换线计划状态
PLAN_ACTIVE = "ACTIVE"
PLAN_SUPERSEDED = "SUPERSEDED"

# 清洁窗口状态
WINDOW_OPEN = "OPEN"
WINDOW_CONCLUDED = "CONCLUDED_PASS"
WINDOW_FAILED = "FAILED"
WINDOW_FROZEN = "FROZEN"
WINDOW_INVALIDATED = "SCOPE_INVALIDATED"
WINDOW_SUPERSEDED = "SUPERSEDED"

# 样本状态
SAMPLE_PENDING = "PENDING"
SAMPLE_RECEIVED = "RECEIVED"
SAMPLE_RESULTED = "RESULTED"
SAMPLE_FROZEN = "FROZEN"

# 步骤状态
STEP_PENDING = "PENDING"
STEP_DONE = "DONE"

# 重算事件类型
EVENT_REWORK = "REWORK"
EVENT_BYPASS = "EQUIPMENT_BYPASS"
EVENT_INSERTION = "UNPLANNED_INSERTION"

# 风险处置触发原因
TRIGGER_SAMPLE_FAIL = "SAMPLE_FAIL"
TRIGGER_SCOPE_CHANGE = "SCOPE_CHANGE"

SCHEMA = """
CREATE TABLE IF NOT EXISTS materials (
    material_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    allergens TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS lines (
    line_id TEXT PRIMARY KEY,
    config TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    line_id TEXT NOT NULL,
    material_id TEXT NOT NULL,
    recipe_version TEXT NOT NULL,
    label_declaration TEXT NOT NULL,
    quantity REAL NOT NULL,
    shipped_qty REAL NOT NULL DEFAULT 0,
    contacted_segments TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sequence_entries (
    line_id TEXT NOT NULL,
    position INTEGER NOT NULL,
    batch_id TEXT NOT NULL,
    occurrence INTEGER NOT NULL,
    PRIMARY KEY (line_id, position)
);
CREATE TABLE IF NOT EXISTS plans (
    plan_id TEXT PRIMARY KEY,
    line_id TEXT NOT NULL,
    from_occ TEXT NOT NULL,
    to_occ TEXT NOT NULL,
    from_batch TEXT NOT NULL,
    to_batch TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS plan_segments (
    plan_id TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    previous_material TEXT,
    next_label_declaration TEXT NOT NULL,
    PRIMARY KEY (plan_id, segment_id)
);
CREATE TABLE IF NOT EXISTS plan_steps (
    plan_id TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    step_key TEXT NOT NULL,
    description TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    completed_by TEXT,
    completed_at TEXT,
    PRIMARY KEY (plan_id, segment_id, step_key)
);
CREATE TABLE IF NOT EXISTS samples (
    sample_no TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    allergen TEXT NOT NULL,
    detection_limit REAL NOT NULL,
    kind TEXT NOT NULL DEFAULT 'REQUIRED',
    status TEXT NOT NULL DEFAULT 'PENDING',
    received_by TEXT,
    received_at TEXT,
    result_value REAL,
    unit TEXT,
    method TEXT,
    issued_by TEXT,
    issued_at TEXT,
    outcome TEXT,
    payload_hash TEXT
);
CREATE TABLE IF NOT EXISTS windows (
    window_id TEXT PRIMARY KEY,
    line_id TEXT NOT NULL,
    plan_id TEXT,
    status TEXT NOT NULL DEFAULT 'OPEN',
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS window_members (
    window_id TEXT NOT NULL,
    occ_key TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    PRIMARY KEY (window_id, occ_key)
);
CREATE TABLE IF NOT EXISTS conclusions (
    conclusion_id TEXT PRIMARY KEY,
    window_id TEXT NOT NULL UNIQUE,
    plan_id TEXT,
    outcome TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS releases (
    release_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL UNIQUE,
    window_id TEXT NOT NULL,
    conclusion_id TEXT NOT NULL,
    recipe_version TEXT NOT NULL,
    label_snapshot TEXT NOT NULL,
    qp_id TEXT NOT NULL,
    exception_ids TEXT NOT NULL DEFAULT '[]',
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS shipments (
    shipment_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    quantity REAL NOT NULL,
    destination TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS risk_dispositions (
    disposition_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    window_id TEXT NOT NULL,
    trigger TEXT NOT NULL,
    shipped_qty REAL NOT NULL,
    reason TEXT NOT NULL,
    original_release_id TEXT,
    notifications TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN',
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS exceptions (
    exception_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    qp_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    waived_samples TEXT NOT NULL,
    waived_steps TEXT NOT NULL,
    allow_isolated_release INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS line_events (
    event_id TEXT PRIMARY KEY,
    line_id TEXT NOT NULL,
    type TEXT NOT NULL,
    payload TEXT NOT NULL,
    affected TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


class DomainError(Exception):
    """领域错误,HTTP 层把它映射为状态码与结构化错误体。"""

    def __init__(self, status, code, message, details=None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details or {}


def _now():
    return datetime.now(timezone.utc).isoformat()


def _uid(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _json(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


def _parse(raw):
    return json.loads(raw)


def _require_text(value, field):
    if not isinstance(value, str) or not value.strip():
        raise DomainError(400, "BAD_REQUEST", f"字段 {field} 必须是非空字符串")
    return value.strip()


def _require_id(value, field):
    value = _require_text(value, field)
    if "@" in value:
        raise DomainError(400, "BAD_REQUEST", f"字段 {field} 不允许包含 '@'")
    return value


class Store:
    """SQLite 持久化存储,提供可重入的事务边界。"""

    def __init__(self, path=":memory:"):
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA)

    @contextmanager
    def transaction(self):
        """一次业务操作一个事务:提交前崩溃即整体回滚。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except Exception:
                self._conn.rollback()
                raise
            else:
                self._conn.commit()

    def close(self):
        with self._lock:
            self._conn.close()


class ChangeoverService:
    """换线放行领域服务。所有写操作都在单个事务内完成。"""

    def __init__(self, store):
        self.store = store

    # ------------------------------------------------------------------
    # 基础读取辅助
    # ------------------------------------------------------------------

    @staticmethod
    def _one(conn, sql, params=()):
        return conn.execute(sql, params).fetchone()

    @staticmethod
    def _all(conn, sql, params=()):
        return conn.execute(sql, params).fetchall()

    def _material(self, conn, material_id):
        row = self._one(conn, "SELECT * FROM materials WHERE material_id = ?", (material_id,))
        if row is None:
            raise DomainError(404, "MATERIAL_NOT_FOUND", f"物料 {material_id} 未登记")
        return row

    def _line(self, conn, line_id):
        row = self._one(conn, "SELECT * FROM lines WHERE line_id = ?", (line_id,))
        if row is None:
            raise DomainError(404, "LINE_NOT_FOUND", f"产线 {line_id} 未登记")
        return row

    def _batch(self, conn, batch_id):
        row = self._one(conn, "SELECT * FROM batches WHERE batch_id = ?", (batch_id,))
        if row is None:
            raise DomainError(404, "BATCH_NOT_FOUND", f"批次 {batch_id} 未登记")
        return row

    def _plan(self, conn, plan_id):
        row = self._one(conn, "SELECT * FROM plans WHERE plan_id = ?", (plan_id,))
        if row is None:
            raise DomainError(404, "PLAN_NOT_FOUND", f"换线计划 {plan_id} 不存在")
        return row

    def _window(self, conn, window_id):
        row = self._one(conn, "SELECT * FROM windows WHERE window_id = ?", (window_id,))
        if row is None:
            raise DomainError(404, "WINDOW_NOT_FOUND", f"清洁窗口 {window_id} 不存在")
        return row

    # ------------------------------------------------------------------
    # 登记:物料 / 产线 / 批次 / 生产顺序
    # ------------------------------------------------------------------

    def register_material(self, payload):
        material_id = _require_id(payload.get("material_id"), "material_id")
        name = _require_text(payload.get("name"), "name")
        allergens = payload.get("allergens") or []
        if not isinstance(allergens, list):
            raise DomainError(400, "BAD_REQUEST", "allergens 必须是数组")
        unknown = sorted(set(allergens) - set(KNOWN_ALLERGENS))
        if unknown:
            raise DomainError(400, "UNKNOWN_ALLERGEN", f"未知过敏原: {unknown}")
        with self.store.transaction() as conn:
            if self._one(conn, "SELECT 1 FROM materials WHERE material_id = ?", (material_id,)):
                raise DomainError(409, "MATERIAL_EXISTS", f"物料 {material_id} 已登记")
            conn.execute(
                "INSERT INTO materials (material_id, name, allergens) VALUES (?, ?, ?)",
                (material_id, name, _json(sorted(set(allergens)))),
            )
        return {"material_id": material_id, "name": name, "allergens": sorted(set(allergens))}

    def register_line(self, payload):
        line_id = _require_id(payload.get("line_id"), "line_id")
        segments = payload.get("segments") or []
        detection_limits = payload.get("detection_limits") or {}
        if not segments:
            raise DomainError(400, "BAD_REQUEST", "产线至少需要一个产线段")
        seen = set()
        normalized = []
        for seg in segments:
            segment_id = _require_id(seg.get("segment_id"), "segment_id")
            if segment_id in seen:
                raise DomainError(400, "BAD_REQUEST", f"产线段重复: {segment_id}")
            seen.add(segment_id)
            steps = []
            for step in seg.get("cleaning_steps") or []:
                steps.append(
                    {
                        "key": _require_id(step.get("key"), "step key"),
                        "description": _require_text(step.get("description"), "step description"),
                    }
                )
            if not steps:
                raise DomainError(400, "BAD_REQUEST", f"产线段 {segment_id} 缺少拆洗步骤")
            normalized.append({"segment_id": segment_id, "cleaning_steps": steps})
        limits = {}
        for allergen, limit in detection_limits.items():
            if allergen not in KNOWN_ALLERGENS:
                raise DomainError(400, "UNKNOWN_ALLERGEN", f"未知过敏原: {allergen}")
            if not isinstance(limit, (int, float)) or limit <= 0:
                raise DomainError(400, "BAD_REQUEST", f"检测限 {allergen} 必须是正数")
            limits[allergen] = float(limit)
        config = {"segments": normalized, "detection_limits": limits}
        with self.store.transaction() as conn:
            if self._one(conn, "SELECT 1 FROM lines WHERE line_id = ?", (line_id,)):
                raise DomainError(409, "LINE_EXISTS", f"产线 {line_id} 已登记")
            conn.execute(
                "INSERT INTO lines (line_id, config) VALUES (?, ?)", (line_id, _json(config))
            )
        return {"line_id": line_id, **config}

    def register_batch(self, payload, conn=None):
        """登记批次。可在事件事务内被复用(传入 conn)。"""
        batch_id = _require_id(payload.get("batch_id"), "batch_id")
        line_id = _require_id(payload.get("line_id"), "line_id")
        material_id = _require_id(payload.get("material_id"), "material_id")
        recipe_version = _require_text(payload.get("recipe_version"), "recipe_version")
        label = payload.get("label_declaration") or {}
        declared = label.get("declared_allergens")
        if not isinstance(declared, list):
            raise DomainError(400, "BAD_REQUEST", "label_declaration.declared_allergens 必须是数组")
        unknown = sorted(set(declared) - set(KNOWN_ALLERGENS))
        if unknown:
            raise DomainError(400, "UNKNOWN_ALLERGEN", f"标签声明了未知过敏原: {unknown}")
        quantity = payload.get("quantity")
        if not isinstance(quantity, (int, float)) or quantity <= 0:
            raise DomainError(400, "BAD_REQUEST", "quantity 必须是正数")

        def _insert(c):
            line = self._line(c, line_id)
            material = self._material(c, material_id)
            config = _parse(line["config"])
            all_segment_ids = [s["segment_id"] for s in config["segments"]]
            contacted = payload.get("contacted_segments")
            if contacted is None:
                contacted = list(all_segment_ids)
            if not isinstance(contacted, list) or not contacted:
                raise DomainError(400, "BAD_REQUEST", "contacted_segments 必须是非空数组")
            unknown_segments = sorted(set(contacted) - set(all_segment_ids))
            if unknown_segments:
                raise DomainError(
                    400, "UNKNOWN_SEGMENT", f"批次接触了未登记的产线段: {unknown_segments}"
                )
            material_allergens = set(_parse(material["allergens"]))
            missing_limits = sorted(material_allergens - set(config["detection_limits"]))
            if missing_limits:
                raise DomainError(
                    409,
                    "DETECTION_LIMIT_MISSING",
                    f"产线 {line_id} 缺少过敏原检测限: {missing_limits}",
                )
            if self._one(c, "SELECT 1 FROM batches WHERE batch_id = ?", (batch_id,)):
                raise DomainError(409, "BATCH_EXISTS", f"批次 {batch_id} 已登记")
            c.execute(
                """INSERT INTO batches
                   (batch_id, line_id, material_id, recipe_version, label_declaration,
                    quantity, shipped_qty, contacted_segments, status, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?)""",
                (
                    batch_id,
                    line_id,
                    material_id,
                    recipe_version,
                    _json(label),
                    float(quantity),
                    _json(contacted),
                    BATCH_PENDING,
                    _now(),
                ),
            )
            return self._batch(c, batch_id)

        if conn is not None:
            row = _insert(conn)
            return self._batch_dict(conn, row)
        with self.store.transaction() as conn:
            row = _insert(conn)
            return self._batch_dict(conn, row)

    def set_sequence(self, line_id, payload):
        """登记真实生产顺序;换线计划只能由它生成。"""
        batch_ids = payload.get("batch_ids")
        if not isinstance(batch_ids, list) or not batch_ids:
            raise DomainError(400, "BAD_REQUEST", "batch_ids 必须是非空数组")
        with self.store.transaction() as conn:
            self._line(conn, line_id)
            for batch_id in batch_ids:
                batch = self._batch(conn, _require_id(batch_id, "batch_id"))
                if batch["line_id"] != line_id:
                    raise DomainError(
                        409, "BATCH_LINE_MISMATCH", f"批次 {batch_id} 不属于产线 {line_id}"
                    )
            self._rewrite_sequence(conn, line_id, batch_ids)
            affected = self._recompute(conn, line_id)
        return {"line_id": line_id, "sequence": list(batch_ids), "affected": affected}

    def _rewrite_sequence(self, conn, line_id, batch_ids):
        conn.execute("DELETE FROM sequence_entries WHERE line_id = ?", (line_id,))
        occurrences = {}
        for position, batch_id in enumerate(batch_ids):
            occurrences[batch_id] = occurrences.get(batch_id, 0) + 1
            conn.execute(
                "INSERT INTO sequence_entries (line_id, position, batch_id, occurrence)"
                " VALUES (?, ?, ?, ?)",
                (line_id, position, batch_id, occurrences[batch_id]),
            )

    # ------------------------------------------------------------------
    # 换线计划生成与清洁窗口重算
    # ------------------------------------------------------------------

    def _profile(self, conn, batch_id):
        batch = self._batch(conn, batch_id)
        material = self._material(conn, batch["material_id"])
        return tuple(sorted(_parse(material["allergens"])))

    @staticmethod
    def _occ_key(entry):
        return f"{entry['batch_id']}@{entry['occurrence']}"

    def _sequence_entries(self, conn, line_id):
        return self._all(
            conn,
            "SELECT * FROM sequence_entries WHERE line_id = ? ORDER BY position",
            (line_id,),
        )

    def _plan_inputs_hash(self, conn, from_entry, to_entry):
        from_batch = self._batch(conn, from_entry["batch_id"])
        to_batch = self._batch(conn, to_entry["batch_id"])
        inputs = {
            "from_batch": from_entry["batch_id"],
            "to_batch": to_entry["batch_id"],
            "from_segments": sorted(_parse(from_batch["contacted_segments"])),
            "to_segments": sorted(_parse(to_batch["contacted_segments"])),
            "to_label": _parse(to_batch["label_declaration"]),
        }
        return hashlib.sha256(_json(inputs).encode("utf-8")).hexdigest()[:10]

    def _recompute(self, conn, line_id):
        """按真实生产顺序重算换线计划与清洁窗口,返回受影响范围。

        计划与窗口的标识由输入内容决定:输入不变则幂等复用,输入变化
        则旧计划被取代;已下结论的窗口若输入变化,则按范围失效处理 —
        未出厂数量隔离,已出厂数量生成风险处置。
        """
        entries = self._sequence_entries(conn, line_id)
        affected = {
            "new_plans": [],
            "superseded_plans": [],
            "new_windows": [],
            "superseded_windows": [],
            "invalidated_windows": [],
            "isolated_batches": [],
            "dispositions": [],
        }
        if not entries:
            return affected

        # 按过敏原画像把顺序切成若干“同画像运行段”,每段是一个清洁窗口。
        runs = []
        for entry in entries:
            profile = self._profile(conn, entry["batch_id"])
            if runs and runs[-1][0] == profile:
                runs[-1][1].append(entry)
            else:
                runs.append((profile, [entry]))

        needed_plans = {}
        needed_windows = {}
        for index, (_profile, run_entries) in enumerate(runs):
            first_occ = self._occ_key(run_entries[0])
            member_occs = [self._occ_key(e) for e in run_entries]
            if index == 0:
                window_key = _json({"line": line_id, "first": first_occ, "plan": None})
                window_id = f"WIN-{line_id}-{hashlib.sha256(window_key.encode()).hexdigest()[:10]}"
                needed_windows[window_id] = {"plan_id": None, "members": member_occs}
                continue
            from_entry = runs[index - 1][1][-1]
            to_entry = run_entries[0]
            from_occ = self._occ_key(from_entry)
            to_occ = self._occ_key(to_entry)
            digest = self._plan_inputs_hash(conn, from_entry, to_entry)
            plan_id = f"PLAN-{line_id}-{from_occ}-{to_occ}-{digest}"
            needed_plans[plan_id] = (from_entry, to_entry)
            window_key = _json({"line": line_id, "first": first_occ, "plan": plan_id,
                                "members": member_occs})
            window_id = f"WIN-{line_id}-{hashlib.sha256(window_key.encode()).hexdigest()[:10]}"
            needed_windows[window_id] = {"plan_id": plan_id, "members": member_occs}

        # 1. 取代不再需要的计划。
        active_plans = self._all(
            conn, "SELECT * FROM plans WHERE line_id = ? AND status = ?", (line_id, PLAN_ACTIVE)
        )
        for plan in active_plans:
            if plan["plan_id"] not in needed_plans:
                conn.execute(
                    "UPDATE plans SET status = ? WHERE plan_id = ?",
                    (PLAN_SUPERSEDED, plan["plan_id"]),
                )
                affected["superseded_plans"].append(plan["plan_id"])

        # 2. 处理不再需要的窗口:未下结论的直接取代;已下结论的按范围失效处理。
        live_windows = self._all(
            conn,
            "SELECT * FROM windows WHERE line_id = ? AND status IN (?, ?, ?)",
            (line_id, WINDOW_OPEN, WINDOW_CONCLUDED, WINDOW_FROZEN),
        )
        for window in live_windows:
            if window["window_id"] in needed_windows:
                continue
            if window["status"] == WINDOW_CONCLUDED:
                self._invalidate_window(
                    conn,
                    window,
                    TRIGGER_SCOPE_CHANGE,
                    "生产顺序变化导致已结论清洁窗口范围失效",
                    affected,
                    WINDOW_INVALIDATED,
                )
            else:
                conn.execute(
                    "UPDATE windows SET status = ? WHERE window_id = ?",
                    (WINDOW_SUPERSEDED, window["window_id"]),
                )
                affected["superseded_windows"].append(window["window_id"])

        # 3. 生成缺失的计划(含按产线段登记的清洁内容)。
        for plan_id, (from_entry, to_entry) in needed_plans.items():
            if self._one(conn, "SELECT 1 FROM plans WHERE plan_id = ?", (plan_id,)):
                continue
            self._create_plan(conn, plan_id, line_id, from_entry, to_entry)
            affected["new_plans"].append(plan_id)

        # 4. 生成缺失的窗口。
        for window_id, spec in needed_windows.items():
            if self._one(conn, "SELECT 1 FROM windows WHERE window_id = ?", (window_id,)):
                continue
            conn.execute(
                "INSERT INTO windows (window_id, line_id, plan_id, status, created_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (window_id, line_id, spec["plan_id"], WINDOW_OPEN, _now()),
            )
            for occ_key in spec["members"]:
                batch_id = occ_key.rsplit("@", 1)[0]
                conn.execute(
                    "INSERT INTO window_members (window_id, occ_key, batch_id) VALUES (?, ?, ?)",
                    (window_id, occ_key, batch_id),
                )
            affected["new_windows"].append(window_id)

        return affected

    def _create_plan(self, conn, plan_id, line_id, from_entry, to_entry):
        """按产线段登记上一批物料、拆洗步骤、验证样本、检测限与下一批标签。"""
        from_batch = self._batch(conn, from_entry["batch_id"])
        to_batch = self._batch(conn, to_entry["batch_id"])
        from_material = self._material(conn, from_batch["material_id"])
        line = self._line(conn, line_id)
        config = _parse(line["config"])
        limits = config["detection_limits"]
        from_segments = set(_parse(from_batch["contacted_segments"]))
        to_segments = _parse(to_batch["contacted_segments"])
        declared = set(_parse(to_batch["label_declaration"]).get("declared_allergens", []))
        from_allergens = set(_parse(from_material["allergens"]))
        # 需要验证的过敏原:上一批带入、且下一批标签未声明的。
        to_verify = sorted(from_allergens - declared)
        now = _now()
        conn.execute(
            """INSERT INTO plans (plan_id, line_id, from_occ, to_occ, from_batch, to_batch,
               status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                plan_id,
                line_id,
                self._occ_key(from_entry),
                self._occ_key(to_entry),
                from_entry["batch_id"],
                to_entry["batch_id"],
                PLAN_ACTIVE,
                now,
            ),
        )
        segment_order = [s["segment_id"] for s in config["segments"]]
        for segment in config["segments"]:
            segment_id = segment["segment_id"]
            if segment_id not in to_segments:
                continue
            previous_material = from_batch["material_id"] if segment_id in from_segments else None
            conn.execute(
                """INSERT INTO plan_segments
                   (plan_id, segment_id, previous_material, next_label_declaration)
                   VALUES (?, ?, ?, ?)""",
                (
                    plan_id,
                    segment_id,
                    previous_material,
                    to_batch["label_declaration"],
                ),
            )
            for step in segment["cleaning_steps"]:
                conn.execute(
                    """INSERT INTO plan_steps
                       (plan_id, segment_id, step_key, description, status)
                       VALUES (?, ?, ?, ?, ?)""",
                    (plan_id, segment_id, step["key"], step["description"], STEP_PENDING),
                )
            if previous_material is None:
                continue
            for allergen in to_verify:
                sample_no = f"SMP-{plan_id}-{segment_id}-{allergen}"
                conn.execute(
                    """INSERT INTO samples
                       (sample_no, plan_id, segment_id, allergen, detection_limit, kind, status)
                       VALUES (?, ?, ?, ?, ?, 'REQUIRED', ?)""",
                    (
                        sample_no,
                        plan_id,
                        segment_id,
                        allergen,
                        float(limits[allergen]),
                        SAMPLE_PENDING,
                    ),
                )
        return plan_id

    def _invalidate_window(self, conn, window, trigger, reason, affected, new_status):
        """窗口失效:隔离未出厂数量,已出厂数量生成可追踪风险处置。"""
        conn.execute(
            "UPDATE windows SET status = ? WHERE window_id = ?",
            (new_status, window["window_id"]),
        )
        if new_status == WINDOW_INVALIDATED:
            affected["invalidated_windows"].append(window["window_id"])
        members = self._all(
            conn,
            "SELECT DISTINCT batch_id FROM window_members WHERE window_id = ?",
            (window["window_id"],),
        )
        for member in members:
            batch = self._batch(conn, member["batch_id"])
            unshipped = batch["quantity"] - batch["shipped_qty"]
            if unshipped > 0 and batch["status"] != BATCH_ISOLATED:
                conn.execute(
                    "UPDATE batches SET status = ? WHERE batch_id = ?",
                    (BATCH_ISOLATED, batch["batch_id"]),
                )
                if batch["batch_id"] not in affected["isolated_batches"]:
                    affected["isolated_batches"].append(batch["batch_id"])
            if batch["shipped_qty"] > 0:
                existing = self._one(
                    conn,
                    """SELECT 1 FROM risk_dispositions
                       WHERE batch_id = ? AND window_id = ? AND trigger = ? AND status = 'OPEN'""",
                    (batch["batch_id"], window["window_id"], trigger),
                )
                if existing:
                    continue
                disposition = self._create_disposition(
                    conn, batch, window["window_id"], trigger, reason
                )
                affected["dispositions"].append(disposition["disposition_id"])

    def _create_disposition(self, conn, batch, window_id, trigger, reason):
        """已出厂部分的风险处置;只新增记录,不改写原检验/放行。"""
        release = self._one(
            conn, "SELECT * FROM releases WHERE batch_id = ?", (batch["batch_id"],)
        )
        shipments = self._all(
            conn, "SELECT * FROM shipments WHERE batch_id = ?", (batch["batch_id"],)
        )
        notifications = [
            {"type": "CUSTOMER", "target": s["destination"], "batch_id": batch["batch_id"]}
            for s in shipments
        ]
        notifications.append({"type": "INTERNAL", "target": "quality", "batch_id": batch["batch_id"]})
        notifications.append(
            {"type": "INTERNAL", "target": "production", "batch_id": batch["batch_id"]}
        )
        disposition_id = _uid("DISP")
        conn.execute(
            """INSERT INTO risk_dispositions
               (disposition_id, batch_id, window_id, trigger, shipped_qty, reason,
                original_release_id, notifications, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)""",
            (
                disposition_id,
                batch["batch_id"],
                window_id,
                trigger,
                batch["shipped_qty"],
                reason,
                release["release_id"] if release else None,
                _json(notifications),
                _now(),
            ),
        )
        return self._one(
            conn, "SELECT * FROM risk_dispositions WHERE disposition_id = ?", (disposition_id,)
        )

    def generate_plans(self, line_id):
        """从真实生产顺序生成/刷新换线计划(幂等)。"""
        with self.store.transaction() as conn:
            self._line(conn, line_id)
            affected = self._recompute(conn, line_id)
            plans = [
                self._plan_dict(conn, row["plan_id"])
                for row in self._all(
                    conn,
                    "SELECT plan_id FROM plans WHERE line_id = ? AND status = ? ORDER BY created_at",
                    (line_id, PLAN_ACTIVE),
                )
            ]
            windows = [
                self._window_dict(conn, row["window_id"])
                for row in self._all(
                    conn,
                    """SELECT window_id FROM windows WHERE line_id = ?
                       AND status != ? ORDER BY created_at""",
                    (line_id, WINDOW_SUPERSEDED),
                )
            ]
        return {"line_id": line_id, "plans": plans, "windows": windows, "affected": affected}

    # ------------------------------------------------------------------
    # 清洁执行与实验室
    # ------------------------------------------------------------------

    def complete_step(self, plan_id, segment_id, step_key, payload):
        operator_id = _require_text(payload.get("operator_id"), "operator_id")
        with self.store.transaction() as conn:
            plan = self._plan(conn, plan_id)
            if plan["status"] != PLAN_ACTIVE:
                raise DomainError(409, "PLAN_SUPERSEDED", f"换线计划 {plan_id} 已被取代")
            step = self._one(
                conn,
                """SELECT * FROM plan_steps
                   WHERE plan_id = ? AND segment_id = ? AND step_key = ?""",
                (plan_id, segment_id, step_key),
            )
            if step is None:
                raise DomainError(
                    404, "STEP_NOT_FOUND", f"计划 {plan_id} 中不存在步骤 {segment_id}/{step_key}"
                )
            if step["status"] == STEP_DONE:
                return self._step_dict(step)  # 重复完成幂等返回
            now = _now()
            conn.execute(
                """UPDATE plan_steps SET status = ?, completed_by = ?, completed_at = ?
                   WHERE plan_id = ? AND segment_id = ? AND step_key = ?""",
                (STEP_DONE, operator_id, now, plan_id, segment_id, step_key),
            )
            step = self._one(
                conn,
                """SELECT * FROM plan_steps
                   WHERE plan_id = ? AND segment_id = ? AND step_key = ?""",
                (plan_id, segment_id, step_key),
            )
            return self._step_dict(step)

    def add_supplementary_sample(self, plan_id, payload):
        """追加确认性样本(例如放行后复测),失败同样触发窗口处置。"""
        sample_no = _require_id(payload.get("sample_no"), "sample_no")
        segment_id = _require_id(payload.get("segment_id"), "segment_id")
        allergen = _require_text(payload.get("allergen"), "allergen")
        if allergen not in KNOWN_ALLERGENS:
            raise DomainError(400, "UNKNOWN_ALLERGEN", f"未知过敏原: {allergen}")
        with self.store.transaction() as conn:
            plan = self._plan(conn, plan_id)
            line = self._line(conn, plan["line_id"])
            limits = _parse(line["config"])["detection_limits"]
            segment = self._one(
                conn,
                "SELECT 1 FROM plan_segments WHERE plan_id = ? AND segment_id = ?",
                (plan_id, segment_id),
            )
            if segment is None:
                raise DomainError(404, "SEGMENT_NOT_FOUND", f"计划 {plan_id} 不含产线段 {segment_id}")
            if self._one(conn, "SELECT 1 FROM samples WHERE sample_no = ?", (sample_no,)):
                raise DomainError(409, "SAMPLE_EXISTS", f"采样编号 {sample_no} 已存在")
            limit = payload.get("detection_limit", limits.get(allergen))
            if limit is None:
                raise DomainError(
                    409, "DETECTION_LIMIT_MISSING", f"产线缺少过敏原 {allergen} 的检测限"
                )
            conn.execute(
                """INSERT INTO samples
                   (sample_no, plan_id, segment_id, allergen, detection_limit, kind, status)
                   VALUES (?, ?, ?, ?, ?, 'SUPPLEMENTARY', ?)""",
                (sample_no, plan_id, segment_id, allergen, float(limit), SAMPLE_PENDING),
            )
            return self._sample_dict(self._sample(conn, sample_no))

    def _sample(self, conn, sample_no):
        row = self._one(conn, "SELECT * FROM samples WHERE sample_no = ?", (sample_no,))
        if row is None:
            raise DomainError(404, "SAMPLE_NOT_FOUND", f"采样编号 {sample_no} 不存在")
        return row

    def receive_sample(self, sample_no, payload):
        """实验室签收。"""
        received_by = _require_text(payload.get("received_by"), "received_by")
        with self.store.transaction() as conn:
            sample = self._sample(conn, sample_no)
            if sample["status"] == SAMPLE_FROZEN:
                raise DomainError(409, "SAMPLE_FROZEN", f"样本 {sample_no} 已冻结")
            if sample["status"] == SAMPLE_PENDING:
                conn.execute(
                    "UPDATE samples SET status = ?, received_by = ?, received_at = ?"
                    " WHERE sample_no = ?",
                    (SAMPLE_RECEIVED, received_by, _now(), sample_no),
                )
                sample = self._sample(conn, sample_no)
            return self._sample_dict(sample)

    @staticmethod
    def _result_hash(sample_no, result_value, unit, method, issued_by):
        payload = {
            "sample_no": sample_no,
            "result_value": float(result_value),
            "unit": unit,
            "method": method,
            "issued_by": issued_by,
        }
        return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()

    def submit_sample_result(self, sample_no, payload):
        """实验室出具结果。

        同一采样编号:内容相同的重传复用原结果;内容不同的重传立即
        冻结样本与所在清洁窗口。结果超过检测限即判定失败,触发窗口
        隔离与已出厂风险处置。
        """
        result_value = payload.get("result_value")
        if not isinstance(result_value, (int, float)):
            raise DomainError(400, "BAD_REQUEST", "result_value 必须是数值")
        unit = _require_text(payload.get("unit"), "unit")
        method = _require_text(payload.get("method"), "method")
        issued_by = _require_text(payload.get("issued_by"), "issued_by")
        digest = self._result_hash(sample_no, result_value, unit, method, issued_by)
        conflict_window_id = None
        with self.store.transaction() as conn:
            sample = self._sample(conn, sample_no)
            if sample["status"] == SAMPLE_FROZEN:
                raise DomainError(409, "SAMPLE_FROZEN", f"样本 {sample_no} 已冻结,等待调查")
            if sample["status"] == SAMPLE_RESULTED:
                if sample["payload_hash"] == digest:
                    result = self._sample_dict(sample)
                    result["reused"] = True
                    return result
                # 异内容重传:立即冻结样本与整个清洁窗口,冻结随事务提交生效。
                conn.execute(
                    "UPDATE samples SET status = ? WHERE sample_no = ?",
                    (SAMPLE_FROZEN, sample_no),
                )
                window = self._window_for_plan(conn, sample["plan_id"])
                if window and window["status"] in (WINDOW_OPEN, WINDOW_CONCLUDED):
                    conn.execute(
                        "UPDATE windows SET status = ? WHERE window_id = ?",
                        (WINDOW_FROZEN, window["window_id"]),
                    )
                conflict_window_id = window["window_id"] if window else None
            else:
                if sample["status"] != SAMPLE_RECEIVED:
                    raise DomainError(
                        409, "SAMPLE_NOT_RECEIVED", f"样本 {sample_no} 尚未由实验室签收"
                    )
                outcome = "PASS" if float(result_value) <= sample["detection_limit"] else "FAIL"
                conn.execute(
                    """UPDATE samples SET status = ?, result_value = ?, unit = ?, method = ?,
                       issued_by = ?, issued_at = ?, outcome = ?, payload_hash = ?
                       WHERE sample_no = ?""",
                    (
                        SAMPLE_RESULTED,
                        float(result_value),
                        unit,
                        method,
                        issued_by,
                        _now(),
                        outcome,
                        digest,
                        sample_no,
                    ),
                )
                affected = None
                if outcome == "FAIL":
                    window = self._window_for_plan(conn, sample["plan_id"])
                    if window and window["status"] in (WINDOW_OPEN, WINDOW_CONCLUDED):
                        affected = {
                            "new_plans": [], "superseded_plans": [], "new_windows": [],
                            "superseded_windows": [], "invalidated_windows": [],
                            "isolated_batches": [], "dispositions": [],
                            "failed_window": window["window_id"],
                        }
                        self._invalidate_window(
                            conn,
                            window,
                            TRIGGER_SAMPLE_FAIL,
                            f"样本 {sample_no} 检出值 {result_value} 超过检测限 "
                            f"{sample['detection_limit']}",
                            affected,
                            WINDOW_FAILED,
                        )
                result = self._sample_dict(self._sample(conn, sample_no))
                result["reused"] = False
                if affected:
                    result["affected"] = affected
                return result
        if conflict_window_id is not None:
            raise DomainError(
                409,
                "SAMPLE_RESULT_CONFLICT",
                f"采样编号 {sample_no} 的重传内容不一致,样本与清洁窗口已冻结",
                {"sample_no": sample_no, "window_id": conflict_window_id},
            )

    def _window_for_plan(self, conn, plan_id):
        return self._one(
            conn,
            """SELECT * FROM windows WHERE plan_id = ?
               ORDER BY created_at DESC LIMIT 1""",
            (plan_id,),
        )

    # ------------------------------------------------------------------
    # 放行 / 出厂 / 例外批准
    # ------------------------------------------------------------------

    def _latest_occurrence(self, conn, batch):
        return self._one(
            conn,
            """SELECT * FROM sequence_entries WHERE line_id = ? AND batch_id = ?
               ORDER BY position DESC LIMIT 1""",
            (batch["line_id"], batch["batch_id"]),
        )

    def _current_window(self, conn, batch):
        entry = self._latest_occurrence(conn, batch)
        if entry is None:
            raise DomainError(
                409, "BATCH_NOT_SCHEDULED", f"批次 {batch['batch_id']} 不在真实生产顺序中"
            )
        occ_key = f"{batch['batch_id']}@{entry['occurrence']}"
        return self._one(
            conn,
            """SELECT w.* FROM windows w
               JOIN window_members m ON m.window_id = w.window_id
               WHERE m.occ_key = ? AND w.status NOT IN (?, ?)
               ORDER BY w.created_at DESC LIMIT 1""",
            (occ_key, WINDOW_SUPERSEDED, WINDOW_INVALIDATED),
        )

    def release_batch(self, batch_id, payload):
        """质量授权人放行。

        清洁结论、配方版本、标签快照与放行记录在同一事务写入;按批次
        与幂等键双重去重,服务恢复后的重试不会产生第二张放行单。
        """
        qp_id = _require_text(payload.get("qp_id"), "qp_id")
        idempotency_key = _require_text(payload.get("idempotency_key"), "idempotency_key")
        with self.store.transaction() as conn:
            batch = self._batch(conn, batch_id)
            existing = self._one(
                conn, "SELECT * FROM releases WHERE batch_id = ?", (batch_id,)
            )
            if existing is not None:
                result = self._release_dict(existing)
                result["replayed"] = True
                return result
            key_holder = self._one(
                conn, "SELECT * FROM releases WHERE idempotency_key = ?", (idempotency_key,)
            )
            if key_holder is not None:
                raise DomainError(
                    409,
                    "IDEMPOTENCY_KEY_CONFLICT",
                    f"幂等键 {idempotency_key} 已用于批次 {key_holder['batch_id']} 的放行",
                )

            exceptions = self._all(
                conn, "SELECT * FROM exceptions WHERE batch_id = ?", (batch_id,)
            )
            allow_isolated = any(e["allow_isolated_release"] for e in exceptions)
            if batch["status"] == BATCH_ISOLATED and not allow_isolated:
                raise DomainError(
                    409,
                    "BATCH_ISOLATED",
                    f"批次 {batch_id} 已隔离,需质量授权人例外批准后方可放行",
                )
            window = self._current_window(conn, batch)
            if window is None:
                raise DomainError(
                    409, "NO_ACTIVE_WINDOW", f"批次 {batch_id} 没有可用的清洁窗口"
                )
            if window["status"] == WINDOW_FROZEN:
                raise DomainError(
                    409, "WINDOW_FROZEN", "清洁窗口存在异内容重传,已冻结等待调查"
                )
            if window["status"] in (WINDOW_FAILED, WINDOW_INVALIDATED) and not allow_isolated:
                raise DomainError(
                    409,
                    "WINDOW_NOT_RELEASABLE",
                    f"清洁窗口状态为 {window['status']},需例外批准后方可放行",
                )

            waived_samples = set()
            waived_steps = set()
            for exception in exceptions:
                waived_samples.update(_parse(exception["waived_samples"]))
                waived_steps.update(_parse(exception["waived_steps"]))

            missing_steps, missing_samples, failed_samples = [], [], []
            plan = None
            if window["plan_id"]:
                plan = self._plan(conn, window["plan_id"])
                if plan["status"] != PLAN_ACTIVE:
                    raise DomainError(
                        409, "PLAN_SUPERSEDED", f"换线计划 {plan['plan_id']} 已被取代"
                    )
                steps = self._all(
                    conn, "SELECT * FROM plan_steps WHERE plan_id = ?", (plan["plan_id"],)
                )
                for step in steps:
                    key = f"{step['segment_id']}:{step['step_key']}"
                    if step["status"] != STEP_DONE and key not in waived_steps:
                        missing_steps.append(key)
                samples = self._all(
                    conn, "SELECT * FROM samples WHERE plan_id = ?", (plan["plan_id"],)
                )
                for sample in samples:
                    if sample["sample_no"] in waived_samples:
                        continue
                    if sample["status"] != SAMPLE_RESULTED:
                        missing_samples.append(sample["sample_no"])
                    elif sample["outcome"] == "FAIL":
                        failed_samples.append(sample["sample_no"])
            if missing_steps or missing_samples or failed_samples:
                raise DomainError(
                    409,
                    "COVERAGE_INCOMPLETE",
                    "清洁覆盖不完整或检测未满足阈值,不能放行",
                    {
                        "missing_steps": missing_steps,
                        "missing_samples": missing_samples,
                        "failed_samples": failed_samples,
                    },
                )

            # 同一事务:清洁结论 + 配方版本 + 标签快照 + 放行记录。
            # 失败/失效窗口不翻状态,历史原样保留,靠放行单上的例外记录说明依据。
            conclusion = self._one(
                conn, "SELECT * FROM conclusions WHERE window_id = ?", (window["window_id"],)
            )
            if conclusion is None:
                conclusion_id = f"CON-{window['window_id']}"
                outcome = "PASS" if not exceptions else "PASS_WITH_EXCEPTION"
                conn.execute(
                    """INSERT INTO conclusions (conclusion_id, window_id, plan_id, outcome,
                       created_at) VALUES (?, ?, ?, ?, ?)""",
                    (conclusion_id, window["window_id"], window["plan_id"], outcome, _now()),
                )
                if window["status"] == WINDOW_OPEN:
                    conn.execute(
                        "UPDATE windows SET status = ? WHERE window_id = ?",
                        (WINDOW_CONCLUDED, window["window_id"]),
                    )
            else:
                conclusion_id = conclusion["conclusion_id"]
            release_id = f"REL-{batch_id}"
            conn.execute(
                """INSERT INTO releases
                   (release_id, batch_id, window_id, conclusion_id, recipe_version,
                    label_snapshot, qp_id, exception_ids, idempotency_key, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    release_id,
                    batch_id,
                    window["window_id"],
                    conclusion_id,
                    batch["recipe_version"],
                    batch["label_declaration"],
                    qp_id,
                    _json([e["exception_id"] for e in exceptions]),
                    idempotency_key,
                    _now(),
                ),
            )
            conn.execute(
                "UPDATE batches SET status = ? WHERE batch_id = ?",
                (BATCH_RELEASED, batch_id),
            )
            result = self._release_dict(
                self._one(conn, "SELECT * FROM releases WHERE release_id = ?", (release_id,))
            )
            result["replayed"] = False
            return result

    def ship_batch(self, batch_id, payload):
        quantity = payload.get("quantity")
        if not isinstance(quantity, (int, float)) or quantity <= 0:
            raise DomainError(400, "BAD_REQUEST", "quantity 必须是正数")
        destination = _require_text(payload.get("destination"), "destination")
        with self.store.transaction() as conn:
            batch = self._batch(conn, batch_id)
            if batch["status"] != BATCH_RELEASED:
                raise DomainError(
                    409, "BATCH_NOT_RELEASED", f"批次 {batch_id} 未放行,不能出厂"
                )
            if batch["shipped_qty"] + quantity > batch["quantity"]:
                raise DomainError(
                    409,
                    "SHIPMENT_EXCEEDS_QUANTITY",
                    f"批次 {batch_id} 可出厂余量不足",
                    {"remaining": batch["quantity"] - batch["shipped_qty"]},
                )
            shipment_id = _uid("SHP")
            conn.execute(
                """INSERT INTO shipments (shipment_id, batch_id, quantity, destination,
                   created_at) VALUES (?, ?, ?, ?, ?)""",
                (shipment_id, batch_id, float(quantity), destination, _now()),
            )
            conn.execute(
                "UPDATE batches SET shipped_qty = shipped_qty + ? WHERE batch_id = ?",
                (float(quantity), batch_id),
            )
            return {
                "shipment_id": shipment_id,
                "batch_id": batch_id,
                "quantity": float(quantity),
                "destination": destination,
            }

    def approve_exception(self, batch_id, payload):
        """质量授权人例外批准,可豁免指定样本/步骤或允许隔离批次放行。"""
        qp_id = _require_text(payload.get("qp_id"), "qp_id")
        reason = _require_text(payload.get("reason"), "reason")
        waived_samples = payload.get("waived_samples") or []
        waived_steps = payload.get("waived_steps") or []
        allow_isolated = bool(payload.get("allow_isolated_release", False))
        with self.store.transaction() as conn:
            self._batch(conn, batch_id)
            exception_id = _uid("EXC")
            conn.execute(
                """INSERT INTO exceptions
                   (exception_id, batch_id, qp_id, reason, waived_samples, waived_steps,
                    allow_isolated_release, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    exception_id,
                    batch_id,
                    qp_id,
                    reason,
                    _json(waived_samples),
                    _json(waived_steps),
                    1 if allow_isolated else 0,
                    _now(),
                ),
            )
            return {
                "exception_id": exception_id,
                "batch_id": batch_id,
                "qp_id": qp_id,
                "reason": reason,
                "waived_samples": list(waived_samples),
                "waived_steps": list(waived_steps),
                "allow_isolated_release": allow_isolated,
            }

    # ------------------------------------------------------------------
    # 返工 / 设备旁路 / 计划外插单
    # ------------------------------------------------------------------

    def record_event(self, line_id, payload):
        """登记产线事件并重新计算受影响范围。"""
        event_type = _require_text(payload.get("type"), "type")
        with self.store.transaction() as conn:
            self._line(conn, line_id)
            if event_type == EVENT_REWORK:
                batch_id = _require_id(payload.get("batch_id"), "batch_id")
                batch = self._batch(conn, batch_id)
                if batch["line_id"] != line_id:
                    raise DomainError(
                        409, "BATCH_LINE_MISMATCH", f"批次 {batch_id} 不属于产线 {line_id}"
                    )
                sequence = [e["batch_id"] for e in self._sequence_entries(conn, line_id)]
                sequence.append(batch_id)  # 返工批次重新上线,排到队尾
                self._rewrite_sequence(conn, line_id, sequence)
            elif event_type == EVENT_BYPASS:
                batch_id = _require_id(payload.get("batch_id"), "batch_id")
                segment_id = _require_id(payload.get("segment_id"), "segment_id")
                batch = self._batch(conn, batch_id)
                if batch["line_id"] != line_id:
                    raise DomainError(
                        409, "BATCH_LINE_MISMATCH", f"批次 {batch_id} 不属于产线 {line_id}"
                    )
                contacted = _parse(batch["contacted_segments"])
                if segment_id not in contacted:
                    raise DomainError(
                        409,
                        "SEGMENT_NOT_CONTACTED",
                        f"批次 {batch_id} 未接触产线段 {segment_id},无法旁路",
                    )
                contacted.remove(segment_id)
                if not contacted:
                    raise DomainError(
                        409, "BYPASS_ALL_SEGMENTS", "不允许旁路批次接触的全部产线段"
                    )
                conn.execute(
                    "UPDATE batches SET contacted_segments = ? WHERE batch_id = ?",
                    (_json(contacted), batch_id),
                )
            elif event_type == EVENT_INSERTION:
                new_batch = payload.get("batch") or {}
                new_batch["line_id"] = line_id
                self.register_batch(new_batch, conn=conn)
                batch_id = new_batch["batch_id"]
                after_batch_id = payload.get("after_batch_id")
                sequence = [e["batch_id"] for e in self._sequence_entries(conn, line_id)]
                if after_batch_id is None:
                    sequence.append(batch_id)
                else:
                    after_batch_id = _require_id(after_batch_id, "after_batch_id")
                    indices = [i for i, b in enumerate(sequence) if b == after_batch_id]
                    if not indices:
                        raise DomainError(
                            404,
                            "SEQUENCE_ANCHOR_NOT_FOUND",
                            f"生产顺序中不存在批次 {after_batch_id}",
                        )
                    sequence.insert(indices[-1] + 1, batch_id)
                self._rewrite_sequence(conn, line_id, sequence)
            else:
                raise DomainError(400, "UNKNOWN_EVENT_TYPE", f"未知事件类型: {event_type}")
            affected = self._recompute(conn, line_id)
            event_id = _uid("EVT")
            conn.execute(
                """INSERT INTO line_events (event_id, line_id, type, payload, affected,
                   created_at) VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    event_id,
                    line_id,
                    event_type,
                    _json(payload),
                    _json(affected),
                    _now(),
                ),
            )
            return {"event_id": event_id, "line_id": line_id,
                    "type": event_type, "affected": affected}

    # ------------------------------------------------------------------
    # 查询与追溯
    # ------------------------------------------------------------------

    def get_batch(self, batch_id):
        with self.store.transaction() as conn:
            batch = self._batch(conn, batch_id)
            result = self._batch_dict(conn, batch)
            try:
                window = self._current_window(conn, batch)
            except DomainError:
                window = None
            if window is not None:
                result["current_window_id"] = window["window_id"]
                result["current_window_status"] = window["status"]
            release = self._one(conn, "SELECT * FROM releases WHERE batch_id = ?", (batch_id,))
            if release is not None:
                result["release_id"] = release["release_id"]
            return result

    def get_plan(self, plan_id):
        with self.store.transaction() as conn:
            return self._plan_dict(conn, plan_id)

    def get_window(self, window_id):
        with self.store.transaction() as conn:
            return self._window_dict(conn, window_id)

    def trace_batch(self, batch_id):
        """从成品反查:换线责任、样本证据、例外批准与通知范围。"""
        with self.store.transaction() as conn:
            batch = self._batch(conn, batch_id)
            windows = self._all(
                conn,
                """SELECT DISTINCT w.* FROM windows w
                   JOIN window_members m ON m.window_id = w.window_id
                   WHERE m.batch_id = ? ORDER BY w.created_at""",
                (batch_id,),
            )
            window_traces = []
            operators, lab_received, lab_issued = set(), set(), set()
            for window in windows:
                entry = {"window": self._window_dict(conn, window["window_id"])}
                if window["plan_id"]:
                    plan = self._plan_dict(conn, window["plan_id"])
                    entry["plan"] = plan
                    for segment in plan["segments"]:
                        for step in segment["cleaning_steps"]:
                            if step.get("completed_by"):
                                operators.add(step["completed_by"])
                        for sample in segment["verification_samples"]:
                            if sample.get("received_by"):
                                lab_received.add(sample["received_by"])
                            if sample.get("issued_by"):
                                lab_issued.add(sample["issued_by"])
                conclusion = self._one(
                    conn,
                    "SELECT * FROM conclusions WHERE window_id = ?",
                    (window["window_id"],),
                )
                if conclusion is not None:
                    entry["conclusion"] = dict(conclusion)
                window_traces.append(entry)
            release = self._one(conn, "SELECT * FROM releases WHERE batch_id = ?", (batch_id,))
            exceptions = [
                self._exception_dict(row)
                for row in self._all(
                    conn, "SELECT * FROM exceptions WHERE batch_id = ?", (batch_id,)
                )
            ]
            dispositions = [
                self._disposition_dict(row)
                for row in self._all(
                    conn, "SELECT * FROM risk_dispositions WHERE batch_id = ?", (batch_id,)
                )
            ]
            shipments = [
                dict(row)
                for row in self._all(
                    conn, "SELECT * FROM shipments WHERE batch_id = ?", (batch_id,)
                )
            ]
            notification_scope = []
            for disposition in dispositions:
                notification_scope.extend(disposition["notifications"])
            return {
                "batch": self._batch_dict(conn, batch),
                "windows": window_traces,
                "release": self._release_dict(release) if release else None,
                "exceptions": exceptions,
                "risk_dispositions": dispositions,
                "shipments": shipments,
                "responsibility": {
                    "operators": sorted(operators),
                    "lab_received_by": sorted(lab_received),
                    "lab_result_by": sorted(lab_issued),
                    "released_by": release["qp_id"] if release else None,
                    "exceptions_approved_by": sorted({e["qp_id"] for e in exceptions}),
                },
                "notification_scope": notification_scope,
            }

    # ------------------------------------------------------------------
    # 序列化
    # ------------------------------------------------------------------

    def _batch_dict(self, conn, row):
        material = self._material(conn, row["material_id"])
        return {
            "batch_id": row["batch_id"],
            "line_id": row["line_id"],
            "material_id": row["material_id"],
            "allergens": _parse(material["allergens"]),
            "recipe_version": row["recipe_version"],
            "label_declaration": _parse(row["label_declaration"]),
            "quantity": row["quantity"],
            "shipped_qty": row["shipped_qty"],
            "contacted_segments": _parse(row["contacted_segments"]),
            "status": row["status"],
        }

    def _plan_dict(self, conn, plan_id):
        plan = self._plan(conn, plan_id)
        segments = []
        for segment in self._all(
            conn,
            "SELECT * FROM plan_segments WHERE plan_id = ? ORDER BY rowid",
            (plan_id,),
        ):
            steps = [
                self._step_dict(row)
                for row in self._all(
                    conn,
                    "SELECT * FROM plan_steps WHERE plan_id = ? AND segment_id = ?",
                    (plan_id, segment["segment_id"]),
                )
            ]
            samples = [
                self._sample_dict(row)
                for row in self._all(
                    conn,
                    "SELECT * FROM samples WHERE plan_id = ? AND segment_id = ?",
                    (plan_id, segment["segment_id"]),
                )
            ]
            segments.append(
                {
                    "segment_id": segment["segment_id"],
                    "previous_material": segment["previous_material"],
                    "next_label_declaration": _parse(segment["next_label_declaration"]),
                    "cleaning_steps": steps,
                    "verification_samples": samples,
                }
            )
        return {
            "plan_id": plan["plan_id"],
            "line_id": plan["line_id"],
            "status": plan["status"],
            "from_batch_id": plan["from_batch"],
            "to_batch_id": plan["to_batch"],
            "from_occurrence": plan["from_occ"],
            "to_occurrence": plan["to_occ"],
            "segments": segments,
        }

    def _window_dict(self, conn, window_id):
        window = self._window(conn, window_id)
        members = [
            {"occurrence": row["occ_key"], "batch_id": row["batch_id"]}
            for row in self._all(
                conn,
                "SELECT * FROM window_members WHERE window_id = ? ORDER BY occ_key",
                (window_id,),
            )
        ]
        conclusion = self._one(
            conn, "SELECT * FROM conclusions WHERE window_id = ?", (window_id,)
        )
        return {
            "window_id": window["window_id"],
            "line_id": window["line_id"],
            "plan_id": window["plan_id"],
            "status": window["status"],
            "members": members,
            "conclusion": dict(conclusion) if conclusion else None,
        }

    @staticmethod
    def _step_dict(row):
        return {
            "segment_id": row["segment_id"],
            "step_key": row["step_key"],
            "description": row["description"],
            "status": row["status"],
            "completed_by": row["completed_by"],
            "completed_at": row["completed_at"],
        }

    @staticmethod
    def _sample_dict(row):
        return {
            "sample_no": row["sample_no"],
            "plan_id": row["plan_id"],
            "segment_id": row["segment_id"],
            "allergen": row["allergen"],
            "detection_limit": row["detection_limit"],
            "kind": row["kind"],
            "status": row["status"],
            "received_by": row["received_by"],
            "result_value": row["result_value"],
            "unit": row["unit"],
            "method": row["method"],
            "issued_by": row["issued_by"],
            "outcome": row["outcome"],
        }

    @staticmethod
    def _release_dict(row):
        return {
            "release_id": row["release_id"],
            "batch_id": row["batch_id"],
            "window_id": row["window_id"],
            "conclusion_id": row["conclusion_id"],
            "recipe_version": row["recipe_version"],
            "label_snapshot": _parse(row["label_snapshot"]),
            "qp_id": row["qp_id"],
            "exception_ids": _parse(row["exception_ids"]),
            "idempotency_key": row["idempotency_key"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _disposition_dict(row):
        return {
            "disposition_id": row["disposition_id"],
            "batch_id": row["batch_id"],
            "window_id": row["window_id"],
            "trigger": row["trigger"],
            "shipped_qty": row["shipped_qty"],
            "reason": row["reason"],
            "original_release_id": row["original_release_id"],
            "notifications": _parse(row["notifications"]),
            "status": row["status"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _exception_dict(row):
        return {
            "exception_id": row["exception_id"],
            "batch_id": row["batch_id"],
            "qp_id": row["qp_id"],
            "reason": row["reason"],
            "waived_samples": _parse(row["waived_samples"]),
            "waived_steps": _parse(row["waived_steps"]),
            "allow_isolated_release": bool(row["allow_isolated_release"]),
            "created_at": row["created_at"],
        }
