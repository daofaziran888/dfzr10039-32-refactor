"""调度认领链路测试：共享作业规格读取、配置版本、暂停恢复与存量计划兼容。"""
from __future__ import annotations

import pytest

import app.scheduler_runtime as runtime
from app.integration import schedule_job, trigger_schedule
from app.models import (
    CrawlJobSpec, SchedulePlan, read_job_spec, read_schedule_plan,
)
from app.scheduler_runtime import (
    ConfigVersionChanged, PlanNotFound, claim_plan, decode_plan,
    execution_manifest, find_bound_job_row, latest_run_record, run_records,
    update_plan_config,
)
from app.services.scheduler import SchedulerService
from app.store import store


# ---------------------------------------------------------------------------
# 测试夹具：本地 J7 作业与 P4 计划
# ---------------------------------------------------------------------------

BAD_CONCURRENCY_ROW = {
    "id": 9001, "status": "待执行", "pending": True, "abnormal": False,
    "作业编号": "J7", "目标站点": "news.example.com",
    "入口URL": "https://news.example.com/old/a, https://news.example.com/old/b",
    "最大深度": "3", "并发线程": "不是数字",
    "调度策略": "interval", "完成页数": "0", "失败页数": "0",
}


@pytest.fixture
def local_j7_p4():
    """登记本地 J7 与 P4，并在用例结束后清理。"""
    runtime.reset_runtime()
    job_row = dict(BAD_CONCURRENCY_ROW)
    store.rows("crawl_job").append(job_row)
    plan_row = {
        "id": 9002, "status": "待激活", "pending": True, "abnormal": False,
        "计划编号": "P4", "关联作业": "J7", "Cron表达式": "0 */6 * * *",
        "时区设置": "Asia/Shanghai", "依赖检查": "", "失败通知": "",
        "并发控制": "", "下次执行": "",
    }
    store.rows("scheduler").append(plan_row)
    try:
        yield job_row, plan_row
    finally:
        store.rows("crawl_job").remove(job_row)
        store.rows("scheduler").remove(plan_row)
        runtime.reset_runtime()


# ---------------------------------------------------------------------------
# 共享读取：输入输出保持原口径
# ---------------------------------------------------------------------------

def test_shared_reader_is_used_by_both_modules() -> None:
    """采集任务与调度编排拿到的是同一份作业规格读取结果。"""
    row = dict(BAD_CONCURRENCY_ROW)

    via_class = CrawlJobSpec.from_seed_row(row)
    via_shared = read_job_spec(row)
    # 调度侧对同一行单独再读一次，字段口径完全一致。
    via_schedule = read_job_spec(dict(row))

    assert (via_class.job_id, via_class.entry_urls, via_class.concurrency) == (
        via_shared.job_id, via_shared.entry_urls, via_shared.concurrency)
    assert via_shared.entry_urls == via_schedule.entry_urls
    assert via_shared.completed_pages == via_schedule.completed_pages == 0
    assert via_shared.concurrency == via_schedule.concurrency == 4
    assert via_shared.job_id == "J7"


def test_shared_reader_passes_through_decoded_spec() -> None:
    """已解码的作业规格再交给共享读取器时原样返回（调度认领复用）。"""
    decoded = CrawlJobSpec.from_seed_row(dict(BAD_CONCURRENCY_ROW))

    assert read_job_spec(decoded) is decoded


def test_shared_reader_matches_original_decoding() -> None:
    """旧字典兼容列名，标准列优先；数字与文本口径不变。"""
    legacy = {
        "任务编号": "J7", "采集入口": "https://a/1,https://a/2",
        "深度": "5", "并发数": "8", "完成页数": "0", "失败页数": "2",
        "作业状态": "采集执行",
    }

    job = read_job_spec(legacy)

    assert job.job_id == "J7"
    assert job.entry_urls == ["https://a/1", "https://a/2"]
    assert (job.max_depth, job.concurrency) == (5, 8)
    assert (job.completed_pages, job.failed_pages) == (0, 2)
    assert job.status == "采集执行"


def test_bad_concurrency_falls_back_to_four_and_zero_pages_kept() -> None:
    """坏文本并发回退到 4，合法的零完成页数保持为 0。"""
    job = read_job_spec(dict(BAD_CONCURRENCY_ROW))

    assert job.concurrency == 4
    assert job.completed_pages == 0
    assert job.failed_pages == 0
    assert job.entry_urls == [
        "https://news.example.com/old/a", "https://news.example.com/old/b"]

    row = dict(BAD_CONCURRENCY_ROW)
    row["完成页数"] = "0"
    assert read_job_spec(row).completed_pages == 0


def test_schedule_plan_keeps_original_timezone() -> None:
    """计划原时区按原口径读取，缺省时才用 Asia/Shanghai。"""
    from_test = read_schedule_plan({
        "计划编号": "P4", "关联作业": "J7", "Cron表达式": "0 * * * *",
        "时区设置": "Asia/Shanghai",
    })
    defaulted = read_schedule_plan({
        "计划编号": "P5", "关联作业": "J7", "Cron表达式": "0 * * * *",
    })

    assert from_test.timezone_setting == "Asia/Shanghai"
    assert defaulted.timezone_setting == "Asia/Shanghai"
    assert SchedulePlan.from_seed_row({"时区设置": "UTC"}).timezone_setting == "UTC"


def test_plan_row_lookup_still_finds_bound_job(local_j7_p4) -> None:
    """重读原计划仍能按关联作业定位绑定的作业。"""
    plan_row = store.find("scheduler", 9002)
    assert plan_row is not None

    job_row = find_bound_job_row(plan_row)
    assert job_row is not None
    assert job_row["作业编号"] == "J7"


# ---------------------------------------------------------------------------
# 认领：执行清单带上同一次配置的地址和时区
# ---------------------------------------------------------------------------

def test_claim_writes_manifest_with_same_config(local_j7_p4) -> None:
    """认领结果落执行清单时，地址与时区来自同一次解码。"""
    snapshot = decode_plan("P4")

    manifest = claim_plan(snapshot)

    assert manifest.address == "https://news.example.com/old/a"
    assert manifest.timezone_setting == "Asia/Shanghai"
    assert manifest.config_version == snapshot.version
    assert manifest.config_digest == snapshot.config_digest
    assert manifest.entry_urls == snapshot.entry_urls
    assert manifest.plan_row_id == 9002 and manifest.job_row_id == 9001

    stored = execution_manifest("P4")
    assert stored["address"] == "https://news.example.com/old/a"
    assert stored["timezone_setting"] == "Asia/Shanghai"


def test_claim_records_full_config(local_j7_p4) -> None:
    """运行记录必须能查到取用的整套配置。"""
    claim_plan("P4")

    record = latest_run_record("P4")
    assert record is not None
    config = record["config"]
    assert config["entry_urls"] == [
        "https://news.example.com/old/a", "https://news.example.com/old/b"]
    assert config["address"] == "https://news.example.com/old/a"
    assert config["timezone_setting"] == "Asia/Shanghai"
    assert config["job_id"] == "J7" and config["plan_id"] == "P4"
    assert config["concurrency"] == 4
    assert config["completed_pages"] == 0
    assert config["config_digest"] == record["config_digest"]


# ---------------------------------------------------------------------------
# 一次配置变更：解码后、认领前暂停，入口与时区一起更新
# ---------------------------------------------------------------------------

def test_pause_between_decode_and_claim_then_update_together(local_j7_p4) -> None:
    """暂停期间把入口与时区一起改掉，恢复后认领得到同一新版本的整套配置。"""
    job_row, plan_row = local_j7_p4
    stale = decode_plan("P4")
    assert stale.entry_urls[0].endswith("/old/a")
    assert stale.timezone_setting == "Asia/Shanghai"

    paused: list[object] = []

    def pause_before_claim(decoded):
        paused.append(decoded)
        raise RuntimeError("值班员暂停操作")

    runtime.BEFORE_CLAIM_HOOK = pause_before_claim
    with pytest.raises(RuntimeError):
        claim_plan(stale)
    assert paused, "应在认领前暂停"
    assert not run_records("P4"), "暂停期间不应产生运行记录"

    # 一次配置变更：入口从旧栏目换到新栏目，同时把计划时区改为 UTC。
    updated = update_plan_config(
        "P4",
        job_values={"入口URL": "https://news.example.com/new/a,https://news.example.com/new/b"},
        plan_values={"时区设置": "UTC"},
    )
    assert updated.version != stale.version
    assert updated.entry_urls == (
        "https://news.example.com/new/a", "https://news.example.com/new/b")
    assert updated.timezone_setting == "UTC"

    runtime.BEFORE_CLAIM_HOOK = None
    manifest = claim_plan(stale)

    # 旧栏目 + 新时区不能拼在一起：认领整体重读为同一版本。
    assert manifest.rebased is True
    assert manifest.previous_version == stale.version
    assert manifest.config_version == updated.version
    assert manifest.entry_urls == updated.entry_urls
    assert manifest.timezone_setting == "UTC"

    record = latest_run_record("P4")
    assert record is not None
    assert record["config_version"] == updated.version
    assert record["config"]["entry_urls"] == list(updated.entry_urls)
    assert record["config"]["timezone_setting"] == "UTC"
    assert record["manifest"]["address"] == "https://news.example.com/new/a"


def test_strict_claim_rejects_stale_snapshot(local_j7_p4) -> None:
    """严格认领遇到版本变化时拒绝，不会拼接两套配置。"""
    stale = decode_plan("P4")
    update_plan_config("P4", plan_values={"时区设置": "UTC"})

    with pytest.raises(ConfigVersionChanged):
        claim_plan(stale, strict=True)

    assert not run_records("P4")


def test_new_run_after_update_uses_full_new_config(local_j7_p4) -> None:
    """恢复后的新执行采用更新后的完整配置。"""
    claim_plan(decode_plan("P4"))
    update_plan_config(
        "P4",
        job_values={"入口URL": "https://news.example.com/new/a"},
        plan_values={"时区设置": "UTC"},
    )

    manifest = trigger_schedule("P4")

    assert manifest.entry_urls == ("https://news.example.com/new/a",)
    assert manifest.address == "https://news.example.com/new/a"
    assert manifest.timezone_setting == "UTC"
    records = run_records("P4")
    assert len(records) == 2
    # 历史运行记录仍保留旧版本，便于追责。
    assert records[0]["config"]["timezone_setting"] == "Asia/Shanghai"
    assert records[1]["config"]["timezone_setting"] == "UTC"


def test_other_existing_plans_still_usable(local_j7_p4) -> None:
    """存量计划照常可用，不受本次变更影响。"""
    job_row = {
        "id": 9003, "status": "待执行", "pending": True, "abnormal": False,
        "作业编号": "J8", "目标站点": "other.example.com",
        "入口URL": "https://other.example.com/1",
        "最大深度": "2", "并发线程": "6",
        "调度策略": "interval", "完成页数": "5", "失败页数": "1",
    }
    plan_row = {
        "id": 9004, "status": "待激活", "pending": True, "abnormal": False,
        "计划编号": "P5", "关联作业": "J8", "Cron表达式": "0 * * * *",
        "时区设置": "Asia/Tokyo", "依赖检查": "", "失败通知": "",
        "并发控制": "", "下次执行": "",
    }
    store.rows("crawl_job").append(job_row)
    store.rows("scheduler").append(plan_row)
    try:
        update_plan_config(
            "P4",
            job_values={"入口URL": "https://news.example.com/new/a"},
            plan_values={"时区设置": "UTC"},
        )

        manifest = trigger_schedule("P5")

        assert manifest.entry_urls == ("https://other.example.com/1",)
        assert manifest.timezone_setting == "Asia/Tokyo"
        assert manifest.concurrency == 6
        assert manifest.completed_pages == 5
    finally:
        store.rows("crawl_job").remove(job_row)
        store.rows("scheduler").remove(plan_row)


# ---------------------------------------------------------------------------
# 原接口：输入输出不变
# ---------------------------------------------------------------------------

def test_schedule_job_still_reuses_registered_plan(local_j7_p4) -> None:
    """schedule_job 仍按原语义复用已登记计划并返回 SchedulePlan。"""
    job = read_job_spec(store.find("crawl_job", 9001))

    plan = schedule_job(job)

    assert plan is not None
    assert plan.plan_id == "P4"
    assert plan.linked_job_id == "J7"
    assert plan.status == "执行中"


def test_scheduler_action_claims_and_keeps_api_shape(local_j7_p4) -> None:
    """资源动作接口返回结构与原来一致，同时完成认领。"""
    service = SchedulerService()

    entry, message = service.run_action(9002, "立即执行")

    assert entry is not None
    assert entry["status"] == "已完成"
    assert entry["pending"] is True  # 原口径：非末位状态仍视为待处理
    assert message == "调度计划已立即执行"
    manifest = execution_manifest("P4")
    assert manifest["address"] == "https://news.example.com/old/a"
    assert manifest["timezone_setting"] == "Asia/Shanghai"


def test_scheduler_action_rejects_unbound_plan(local_j7_p4) -> None:
    """未关联作业的计划不认领，返回原风格的失败信息。"""
    plan_row = store.find("scheduler", 9002)
    assert plan_row is not None
    plan_row.pop("关联作业", None)

    service = SchedulerService()
    entry, message = service.run_action(9002, "激活计划")

    assert entry is None
    assert "未关联作业" in message
    assert not run_records("P4")


def test_decode_missing_plan_raises() -> None:
    """不存在的计划给出明确错误。"""
    with pytest.raises(PlanNotFound):
        decode_plan("NOT-EXIST")


# ---------------------------------------------------------------------------
# 旧栏目迁移：同一列换名，入口与时区仍来自同一次配置
# ---------------------------------------------------------------------------

def test_entry_column_migration_keeps_one_config(local_j7_p4) -> None:
    """入口从旧列名换到新列名，一次变更后认领仍取整套新配置。"""
    job_row, _ = local_j7_p4
    job_row["入口"] = job_row.pop("入口URL")  # 模拟历史数据放在旧栏目

    before = decode_plan("P4")
    assert before.entry_urls == (
        "https://news.example.com/old/a", "https://news.example.com/old/b")
    assert before.entry_field == "入口"

    after = update_plan_config(
        "P4",
        job_values={
            "入口": None,  # 收拢旧栏目
            "入口URL": "https://news.example.com/new/a",  # 标准列换上新栏目
        },
        plan_values={"时区设置": "UTC"},
    )

    assert after.entry_field == "入口URL"
    assert after.entry_urls == ("https://news.example.com/new/a",)
    assert after.timezone_setting == "UTC"
    assert after.version != before.version

    manifest = claim_plan(before)
    assert manifest.config_version == after.version
    assert manifest.entry_urls == after.entry_urls
    assert manifest.timezone_setting == after.timezone_setting
    assert manifest.address == "https://news.example.com/new/a"


# ---------------------------------------------------------------------------
# 等价性：共享读取器与旧的内联解码逐字段一致
# ---------------------------------------------------------------------------

def _legacy_decode_job(row):
    """重构前 CrawlJobSpec.from_seed_row 的内联逻辑，作为文档化基线。"""
    raw_url = str(row.get("入口URL", ""))
    urls = [u.strip() for u in raw_url.split(",") if u.strip()] if raw_url else []
    parsed = {}
    for key, default in (
        ("最大深度", 3), ("并发线程", 4), ("完成页数", 0), ("失败页数", 0),
    ):
        try:
            parsed[key] = int(row.get(key, default))
        except (ValueError, TypeError):
            parsed[key] = default
    return {
        "job_id": str(row.get("作业编号", "")),
        "entry_urls": urls,
        "max_depth": parsed["最大深度"],
        "concurrency": parsed["并发线程"],
        "schedule_policy": str(row.get("调度策略", "interval")),
        "completed_pages": parsed["完成页数"],
        "failed_pages": parsed["失败页数"],
        "status": str(row.get("status", "待执行")),
    }


@pytest.mark.parametrize("row", [
    dict(BAD_CONCURRENCY_ROW),
    {"作业编号": "J1", "入口URL": "https://a/1, ,https://a/2", "最大深度": "0",
     "并发线程": "0", "完成页数": "0", "失败页数": "0", "status": "待执行"},
    {"作业编号": "J2", "入口URL": "", "最大深度": "x", "并发线程": None,
     "完成页数": "12", "失败页数": "oops", "调度策略": "cron",
     "status": "采集执行"},
    {"作业编号": "J3", "入口URL": "   ", "最大深度": "3.7", "并发线程": True,
     "完成页数": "-1", "失败页数": "2"},
    {},
])
def test_shared_reader_matches_legacy_decoding(row) -> None:
    """标准列上与旧解码逻辑逐字段一致。"""
    legacy = _legacy_decode_job(row)
    reader = read_job_spec(dict(row))

    assert reader.job_id == legacy["job_id"]
    assert reader.entry_urls == legacy["entry_urls"]
    assert reader.max_depth == legacy["max_depth"]
    assert reader.concurrency == legacy["concurrency"]
    assert reader.completed_pages == legacy["completed_pages"]
    assert reader.failed_pages == legacy["failed_pages"]
    # 文本字段：缺失键按默认值兜底，与旧逻辑一致。
    if "调度策略" in row:
        assert reader.schedule_policy == legacy["schedule_policy"]
    if "status" in row:
        assert reader.status == legacy["status"]
