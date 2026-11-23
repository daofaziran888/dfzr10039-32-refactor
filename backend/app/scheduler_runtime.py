"""调度认领运行时：作业规格快照、配置版本校验与执行清单/运行记录。

采集任务和调度编排都通过 :mod:`app.models` 里的共享读取函数解码旧字典，
本模块只负责把一次解码结果固定成带版本的配置快照。认领（claim）时必须
重新核对当前配置版本：

- 版本一致：沿用已解码快照；
- 版本已变：整份重新解码，绝不把旧入口和新时区拼在一起；
- 每次认领都写入执行清单和运行记录，记录取用的整套配置。
"""
from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

from app.models import (
    DEFAULT_TIMEZONE,
    JOB_FIELD_ALIASES,
    PLAN_FIELD_ALIASES,
    CrawlJobSpec,
    SchedulePlan,
    read_field,
    read_int,
    read_job_spec,
    read_schedule_plan,
    read_text,
    read_url_list,
)
from app.store import store


_LOCK = threading.RLock()

#: 测试/运维可以在解码完成之后、真正认领之前暂停在这里。
#: 回调收到解码快照；抛异常即中止本次认领，恢复后再次 claim 会重新核对版本。
BEFORE_CLAIM_HOOK: Any = None


class PlanNotFound(LookupError):
    """计划或计划绑定的作业不存在。"""


class ConfigVersionChanged(RuntimeError):
    """严格认领时发现配置版本已经变化。"""

    def __init__(self, expected: str, actual: str) -> None:
        super().__init__(f"配置版本已变化：解码时 {expected}，当前 {actual}")
        self.expected = expected
        self.actual = actual


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def _digest(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical(payload).encode()).hexdigest()


@dataclass(frozen=True)
class PlanConfigSnapshot:
    """一次解码得到的完整配置快照；入口、时区和作业字段属于同一版本。"""

    plan_id: str
    job_id: str
    version: str
    entry_urls: tuple[str, ...]
    timezone_setting: str
    cron_expr: str
    concurrency: int
    max_depth: int
    schedule_policy: str
    completed_pages: int
    failed_pages: int
    plan_status: str
    job_status: str
    plan_row_id: int
    job_row_id: int
    entry_field: str
    timezone_field: str
    config_digest: str
    decoded_at: str = field(default_factory=_now)

    @property
    def address(self) -> str:
        """执行清单里的采集地址：取入口清单第一项。"""
        return self.entry_urls[0] if self.entry_urls else ""

    @property
    def entries(self) -> list[str]:
        return list(self.entry_urls)

    def config(self) -> dict[str, Any]:
        """运行记录里保存的整套配置。"""
        return {
            "plan_id": self.plan_id,
            "job_id": self.job_id,
            "config_version": self.version,
            "config_digest": self.config_digest,
            "entry_urls": list(self.entry_urls),
            "address": self.address,
            "timezone_setting": self.timezone_setting,
            "cron_expr": self.cron_expr,
            "concurrency": self.concurrency,
            "max_depth": self.max_depth,
            "schedule_policy": self.schedule_policy,
            "completed_pages": self.completed_pages,
            "failed_pages": self.failed_pages,
            "plan_status": self.plan_status,
            "job_status": self.job_status,
            "entry_field": self.entry_field,
            "timezone_field": self.timezone_field,
            "plan_row_id": self.plan_row_id,
            "job_row_id": self.job_row_id,
        }

    def as_dict(self) -> dict[str, Any]:
        payload = self.config()
        payload["decoded_at"] = self.decoded_at
        return payload

    # 兼容按字典方式读取快照的调用方。
    def to_dict(self) -> dict[str, Any]:
        return self.as_dict()

    def __getitem__(self, key: str) -> Any:
        return self.as_dict()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.as_dict().get(key, default)

    def keys(self):
        return self.as_dict().keys()


@dataclass(frozen=True)
class ExecutionManifest:
    """执行清单条目：认领结果，地址和时区来自同一次配置。"""

    manifest_id: str
    plan_id: str
    job_id: str
    config_version: str
    config_digest: str
    entry_urls: tuple[str, ...]
    address: str
    timezone_setting: str
    cron_expr: str
    concurrency: int
    max_depth: int
    completed_pages: int
    failed_pages: int
    plan_row_id: int
    job_row_id: int
    claimed_at: str = field(default_factory=_now)
    rebased: bool = False
    previous_version: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "manifest_id": self.manifest_id,
            "plan_id": self.plan_id,
            "job_id": self.job_id,
            "config_version": self.config_version,
            "config_digest": self.config_digest,
            "entry_urls": list(self.entry_urls),
            "address": self.address,
            "timezone_setting": self.timezone_setting,
            "cron_expr": self.cron_expr,
            "concurrency": self.concurrency,
            "max_depth": self.max_depth,
            "completed_pages": self.completed_pages,
            "failed_pages": self.failed_pages,
            "plan_row_id": self.plan_row_id,
            "job_row_id": self.job_row_id,
            "claimed_at": self.claimed_at,
            "rebased": self.rebased,
            "previous_version": self.previous_version,
        }


@dataclass(frozen=True)
class RunRecord:
    """运行记录：完整保存本次认领取用的整套配置和来源版本。"""

    run_id: str
    plan_id: str
    job_id: str
    config_version: str
    config_digest: str
    config: dict[str, Any]
    manifest: dict[str, Any]
    created_at: str = field(default_factory=_now)
    rebased: bool = False
    previous_version: str = ""
    status: str = "已认领"

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "plan_id": self.plan_id,
            "job_id": self.job_id,
            "config_version": self.config_version,
            "config_digest": self.config_digest,
            "config": dict(self.config),
            "manifest": dict(self.manifest),
            "created_at": self.created_at,
            "rebased": self.rebased,
            "previous_version": self.previous_version,
            "status": self.status,
        }


_MANIFESTS: dict[str, ExecutionManifest] = {}
_RUNS: list[RunRecord] = []
_RUN_SEQ = 0


# ---------------------------------------------------------------------------
# 行定位：兼容计划编号与数据库 id，兼容旧字典列名
# ---------------------------------------------------------------------------

def _plan_row_identifier(row: dict[str, Any]) -> str:
    return read_text(row, PLAN_FIELD_ALIASES["plan_id"])


def _job_row_identifier(row: dict[str, Any]) -> str:
    return read_text(row, JOB_FIELD_ALIASES["job_id"])


def resolve_plan_row(identifier: str | int) -> dict[str, Any]:
    """按计划编号或行 id 定位已登记计划。"""
    text = str(identifier).strip()
    rows = store.rows("scheduler")
    if isinstance(identifier, int) or text.isdigit():
        row = store.find("scheduler", int(text))
        if row is not None:
            return row
    for row in rows:
        if _plan_row_identifier(row) == text:
            return row
    raise PlanNotFound(f"调度计划 {identifier} 不存在或已归档")


def find_bound_job_row(plan: SchedulePlan | dict[str, Any] | str | int) -> dict[str, Any] | None:
    """重读计划仍按“关联作业”定位绑定作业，兼容旧列名。

    也可以直接传计划编号/行 id，内部先定位已登记计划。
    """
    if isinstance(plan, (str, int)):
        plan = resolve_plan_row(plan)
    parsed = read_schedule_plan(plan)
    if not parsed.linked_job_id:
        return None
    for row in store.rows("crawl_job"):
        if _job_row_identifier(row) == parsed.linked_job_id:
            return row
    return None


def _entry_field_used(row: dict[str, Any]) -> str:
    for name in JOB_FIELD_ALIASES["entry_urls"]:
        if read_field(row, (name,), None) is not None:
            return name
    return ""


def _timezone_field_used(row: dict[str, Any]) -> str:
    for name in PLAN_FIELD_ALIASES["timezone_setting"]:
        if read_field(row, (name,), None) is not None:
            return name
    return ""


# ---------------------------------------------------------------------------
# 解码
# ---------------------------------------------------------------------------

def _build_snapshot(plan_row: dict[str, Any], job_row: dict[str, Any] | None) -> PlanConfigSnapshot:
    plan = read_schedule_plan(plan_row)
    job = read_job_spec(job_row) if job_row is not None else read_job_spec({})

    entry_urls = tuple(job.entry_urls)
    timezone_setting = plan.timezone_setting or DEFAULT_TIMEZONE
    payload = {
        "plan_id": plan.plan_id or _plan_row_identifier(plan_row),
        "job_id": plan.linked_job_id or job.job_id,
        "entry_urls": list(entry_urls),
        "timezone_setting": timezone_setting,
        "cron_expr": plan.cron_expr,
        "concurrency": job.concurrency,
        "max_depth": job.max_depth,
        "schedule_policy": job.schedule_policy,
    }
    digest = _digest(payload)
    explicit_version = read_text(plan_row, PLAN_FIELD_ALIASES["config_version"], "")
    version = f"{explicit_version}-{digest[:12]}" if explicit_version else f"cfg-{digest[:12]}"

    return PlanConfigSnapshot(
        plan_id=payload["plan_id"],
        job_id=payload["job_id"],
        version=version,
        entry_urls=entry_urls,
        timezone_setting=timezone_setting,
        cron_expr=plan.cron_expr,
        concurrency=job.concurrency,
        max_depth=job.max_depth,
        schedule_policy=job.schedule_policy,
        completed_pages=job.completed_pages,
        failed_pages=job.failed_pages,
        plan_status=plan.status,
        job_status=job.status,
        plan_row_id=int(plan_row.get("id", 0)),
        job_row_id=int(job_row.get("id", 0)) if job_row is not None else 0,
        entry_field=_entry_field_used(job_row or {}),
        timezone_field=_timezone_field_used(plan_row),
        config_digest=digest,
    )


def _decode_locked(identifier: str | int) -> PlanConfigSnapshot:
    """在锁内解码；双读校验避免读到一半的并发配置变更。"""
    last_error: Exception | None = None
    for _ in range(3):
        plan_row = resolve_plan_row(identifier)
        job_row = find_bound_job_row(plan_row)
        first = _build_snapshot(plan_row, job_row)

        plan_row_again = resolve_plan_row(identifier)
        job_row_again = find_bound_job_row(plan_row_again)
        second = _build_snapshot(plan_row_again, job_row_again)
        if first.config_digest == second.config_digest:
            return second
        last_error = RuntimeError("计划配置在解码过程中发生变化，请重试")
    raise last_error or RuntimeError("无法解析调度计划配置")


def decode_plan(identifier: str | int) -> PlanConfigSnapshot:
    """解码计划的完整配置，返回带版本的快照。"""
    # 未登记作业时仍按空规格兜底，已有计划不会因为绑定缺失而不可用。
    resolve_plan_row(identifier)
    with _LOCK:
        return _decode_locked(identifier)


# ---------------------------------------------------------------------------
# 认领
# ---------------------------------------------------------------------------

def _next_run_id() -> str:
    global _RUN_SEQ
    _RUN_SEQ += 1
    return f"RUN-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{_RUN_SEQ:04d}"


def claim_plan(
    plan: PlanConfigSnapshot | str | int,
    *,
    strict: bool = False,
) -> ExecutionManifest:
    """认领计划：以同一份配置生成执行清单。

    ``plan`` 可以是解码快照，也可以是计划编号/行 id。传入快照且当前配置
    已经变化时，默认整份重新解码（不会拼出旧入口+新时区），``strict=True``
    则拒绝认领并抛出 :class:`ConfigVersionChanged`。
    """
    with _LOCK:
        if isinstance(plan, PlanConfigSnapshot):
            decoded = plan
            identifier: str | int = plan.plan_id or plan.plan_row_id
        else:
            identifier = plan
            decoded = _decode_locked(identifier)

        if BEFORE_CLAIM_HOOK is not None:
            BEFORE_CLAIM_HOOK(decoded)

        current = _decode_locked(identifier)
        if current.config_digest != decoded.config_digest:
            if strict:
                raise ConfigVersionChanged(decoded.version, current.version)
            used = current
            rebased = True
        else:
            used = current
            rebased = False

        manifest = ExecutionManifest(
            manifest_id=f"MANIFEST-{used.plan_id}-{_next_run_id()}",
            plan_id=used.plan_id,
            job_id=used.job_id,
            config_version=used.version,
            config_digest=used.config_digest,
            entry_urls=used.entry_urls,
            address=used.address,
            timezone_setting=used.timezone_setting,
            cron_expr=used.cron_expr,
            concurrency=used.concurrency,
            max_depth=used.max_depth,
            completed_pages=used.completed_pages,
            failed_pages=used.failed_pages,
            plan_row_id=used.plan_row_id,
            job_row_id=used.job_row_id,
            rebased=rebased,
            previous_version=decoded.version if rebased else "",
        )
        record = RunRecord(
            run_id=_next_run_id(),
            plan_id=used.plan_id,
            job_id=used.job_id,
            config_version=used.version,
            config_digest=used.config_digest,
            config=used.config(),
            manifest=manifest.as_dict(),
            rebased=rebased,
            previous_version=decoded.version if rebased else "",
        )
        _MANIFESTS[used.plan_id] = manifest
        _RUNS.append(record)
        return manifest


def run_plan(identifier: str | int) -> ExecutionManifest:
    """先解码再认领；等价于调度触发一次计划。"""
    return claim_plan(decode_plan(identifier))


# 便于调用方按语义选择名字。
decode_schedule_plan = decode_plan
claim_schedule_plan = claim_plan
run_schedule_plan = run_plan


# ---------------------------------------------------------------------------
# 执行清单与运行记录
# ---------------------------------------------------------------------------

def execution_manifest(identifier: str | int | None = None) -> dict[str, Any] | list[dict[str, Any]]:
    """读取执行清单：不传参数返回全部，传计划则返回最新一条。"""
    with _LOCK:
        if identifier is None:
            return [manifest.as_dict() for manifest in _MANIFESTS.values()]
        plan_id = _plan_id_of(identifier)
        manifest = _MANIFESTS.get(plan_id)
        return manifest.as_dict() if manifest is not None else {}


def run_records(
    identifier: str | int | None = None,
    *,
    config_version: str | None = None,
) -> list[dict[str, Any]]:
    """读取运行记录；每条都带本次取用的整套配置。"""
    plan_id = _plan_id_of(identifier) if identifier is not None else None
    with _LOCK:
        records: Iterable[RunRecord] = _RUNS
        result = [
            record.as_dict() for record in records
            if (plan_id is None or record.plan_id == plan_id)
            and (config_version is None or record.config_version == config_version)
        ]
    return result


def latest_run_record(identifier: str | int) -> dict[str, Any] | None:
    records = run_records(identifier)
    return records[-1] if records else None


def _plan_id_of(identifier: str | int) -> str:
    if isinstance(identifier, PlanConfigSnapshot):
        return identifier.plan_id
    text = str(identifier).strip()
    if text.isdigit():
        row = store.find("scheduler", int(text))
        if row is not None:
            return _plan_row_identifier(row)
    return text


# ---------------------------------------------------------------------------
# 一次配置变更：入口与时区同版本更新
# ---------------------------------------------------------------------------

def update_plan_config(
    identifier: str | int,
    *,
    job_values: dict[str, Any] | None = None,
    plan_values: dict[str, Any] | None = None,
    version: str | None = None,
) -> PlanConfigSnapshot:
    """把作业侧（如入口栏目）和计划侧（如时区）作为一次变更原子写入。

    值为 ``None`` 表示删除该旧栏目，便于把入口从旧列名迁移到新列名。
    变更完成后返回重新解码的完整快照，入口与时区来自同一个版本。
    """
    with _LOCK:
        plan_row = resolve_plan_row(identifier)
        job_row = find_bound_job_row(plan_row)

        if plan_values:
            for field, value in plan_values.items():
                if value is None:
                    plan_row.pop(field, None)
                else:
                    plan_row[field] = value
            # 换绑作业时重新定位；未换绑则沿用已定位的作业行。
            job_row = find_bound_job_row(plan_row) or job_row

        if job_values:
            if job_row is None:
                raise PlanNotFound(
                    f"调度计划 {_plan_row_identifier(plan_row)} 未绑定可更新的作业")
            for field, value in job_values.items():
                if value is None:
                    job_row.pop(field, None)
                else:
                    job_row[field] = value

        # 显式版本只作为版本前缀；不传则保留原有版本列。
        # 内容摘要始终参与版本计算，配置一变版本必然变化。
        if version is not None:
            plan_row["配置版本"] = version

        plan_identifier = _plan_row_identifier(plan_row) or plan_row.get("id", identifier)
        return _decode_locked(plan_identifier)


def reset_runtime() -> None:
    """清空执行清单、运行记录和暂停钩子（测试用）。"""
    global BEFORE_CLAIM_HOOK, _RUN_SEQ
    with _LOCK:
        _MANIFESTS.clear()
        _RUNS.clear()
        _RUN_SEQ = 0
        BEFORE_CLAIM_HOOK = None
