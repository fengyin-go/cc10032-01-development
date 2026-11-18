"""窗口指标台账领域服务：另一个业务模块，随窗口指标落地同步生成台账。

每条窗口分组指标记录在事务内落地的同时，会在本模块同步登记一条台账，
保证「指标结果—落库回执—台账」三者版本一致；事务回滚时台账同步撤销。
"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.window_metric import LEDGER_TABLE
from app.store import store


class MetricResultLedgerService(BaseModuleService):
    MODULE = LEDGER_TABLE
    ENTITY = "窗口指标台账"
    LIST_FIELDS = [
        "台账编号", "来源批次", "窗口分组", "指标版本",
        "事件条数", "结果摘要", "落地回执",
    ]
    REQUIRED_FIELDS = ["台账编号", "来源批次", "窗口分组"]
    STATUS_ORDER = ["已登记"]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    def list_by_batch(self, batch_id: str) -> list[dict]:
        return [
            row for row in store.rows(self.MODULE)
            if row.get("来源批次") == batch_id
        ]

    def domain_snapshot(self) -> dict:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["ledger_entries"] = len(rows)
        base["versions_covered"] = len({r.get("指标版本") for r in rows})
        return base
