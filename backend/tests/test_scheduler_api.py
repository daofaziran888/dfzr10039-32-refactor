"""资源接口回归：调度动作认领后原接口输入输出不变，并额外落执行清单/运行记录。"""
from __future__ import annotations

import asyncio
import json

import pytest

import app.scheduler_runtime as runtime
from app.main import app
from app.store import store


def _request(method: str, path: str, **kwargs):
    scope = {
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
        "method": method, "scheme": "http", "path": path,
        "raw_path": path.encode(), "root_path": "", "query_string": b"",
        "headers": [(b"host", b"testserver"), (b"content-type", b"application/json")],
        "client": ("127.0.0.1", 1234), "server": ("testserver", 80),
    }
    body = json.dumps(kwargs.get("json", {})).encode() if "json" in kwargs else b""
    sent = False
    messages: list[dict] = []

    async def receive():
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message):
        messages.append(message)

    asyncio.run(app(scope, receive, send))

    status_code = next(m["status"] for m in messages if m["type"] == "http.response.start")
    payload = b"".join(
        m.get("body", b"") for m in messages if m["type"] == "http.response.body")
    return status_code, payload


@pytest.fixture
def j7_p4():
    runtime.reset_runtime()
    job_row = {
        "id": 9301, "status": "待执行", "pending": True, "abnormal": False,
        "作业编号": "J7", "目标站点": "news.example.com",
        "入口URL": "https://news.example.com/old/a",
        "最大深度": "3", "并发线程": "??", "调度策略": "interval",
        "完成页数": "0", "失败页数": "0",
    }
    plan_row = {
        "id": 9302, "status": "待激活", "pending": True, "abnormal": False,
        "计划编号": "P4", "关联作业": "J7", "Cron表达式": "0 */6 * * *",
        "时区设置": "Asia/Shanghai", "依赖检查": "", "失败通知": "",
        "并发控制": "", "下次执行": "",
    }
    store.rows("crawl_job").append(job_row)
    store.rows("scheduler").append(plan_row)
    try:
        yield job_row, plan_row
    finally:
        store.rows("crawl_job").remove(job_row)
        store.rows("scheduler").remove(plan_row)
        runtime.reset_runtime()


def test_list_scheduler_contract_unchanged(j7_p4) -> None:
    status, payload = _request("GET", "/api/scheduler")

    assert status == 200
    body = json.loads(payload)
    assert set(body) == {"items", "total", "page", "size"}
    row = next(item for item in body["items"] if item["计划编号"] == "P4")
    assert set(row) >= {
        "id", "status", "pending", "abnormal", "计划编号", "关联作业",
        "Cron表达式", "时区设置", "依赖检查", "失败通知", "并发控制", "下次执行"}


def test_action_contract_unchanged_and_claim_recorded(j7_p4) -> None:
    status, payload = _request(
        "POST", "/api/scheduler/9302/actions", json={"action": "立即执行"})

    assert status == 200
    body = json.loads(payload)
    assert set(body) == {"ok", "message", "entry"}
    assert body["ok"] is True
    assert body["entry"]["status"] == "已完成"
    assert body["entry"]["计划编号"] == "P4"

    manifest = runtime.execution_manifest("P4")
    record = runtime.latest_run_record("P4")
    assert manifest["address"] == "https://news.example.com/old/a"
    assert manifest["timezone_setting"] == "Asia/Shanghai"
    assert record["config"]["address"] == "https://news.example.com/old/a"
    assert record["config"]["timezone_setting"] == "Asia/Shanghai"


def test_get_entry_contract_unchanged(j7_p4) -> None:
    status, payload = _request("GET", "/api/scheduler/9302")

    assert status == 200
    body = json.loads(payload)
    assert body["计划编号"] == "P4"
    assert body["时区设置"] == "Asia/Shanghai"
