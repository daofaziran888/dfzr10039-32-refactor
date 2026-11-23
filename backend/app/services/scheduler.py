"""调度编排领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。

调度动作额外做两件事：以作业规格读取结果认领已登记计划，并把认领结果写进
执行清单和运行记录；资源接口的返回结构保持不变。
"""
from __future__ import annotations

from typing import Any

from app.models import read_schedule_plan
from app.scheduler_runtime import claim_plan
from app.services.base import BaseModuleService
from app.store import store


class SchedulerService(BaseModuleService):
    MODULE = "scheduler"
    ENTITY = "调度计划"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["计划编号", "关联作业", "Cron表达式", "时区设置", "依赖检查", "失败通知", "并发控制", "下次执行"]
    REQUIRED_FIELDS = ["计划编号", "关联作业", "Cron表达式"]
    STATUS_ORDER = ["待激活", "执行中", "已完成", "已停用"]
    ACTION_RULES = {"激活计划": "执行中", "立即执行": "已完成", "停用计划": "已停用"}
    NEGATIVE_ACTIONS = ["停用计划"]
    CLAIM_ACTIONS = {"激活计划", "立即执行"}

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action in self.ACTION_RULES and action in self.CLAIM_ACTIONS:
            plan = read_schedule_plan(entry)
            if not plan.linked_job_id:
                return None, f"{self.ENTITY}未关联作业，无法认领采集入口"
            try:
                claim_plan(entry_id)
            except LookupError as exc:
                return None, str(exc)
        return super().run_action(entry_id, action)
