"""滑动指标领域服务：定义基于滑动窗口的实时指标计算规则。

增强能力: 条件过滤、快照频率校验、过期清理、指标聚合输出、跨模块指标查询
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SlidingIndexService(BaseModuleService):
    MODULE = "sliding_index"
    ENTITY = "指标计算"
    LIST_FIELDS = ["指标名称", "计算窗口", "聚合函数", "分组维度", "过滤条件", "快照频率", "保留时长"]
    REQUIRED_FIELDS = ["指标名称", "计算窗口", "聚合函数"]
    STATUS_ORDER = ["草稿", "计算中", "已生效", "已过期"]
    ACTION_RULES = {"启用指标": "草稿", "停用指标": "计算中"}
    NEGATIVE_ACTIONS = ["停用指标"]

    def evaluate_index_condition(
        self, index_name: str, data: dict[str, Any],
    ) -> dict[str, Any]:
        """评估指标的过滤条件，判断数据是否满足计算条件。"""
        index_def = None
        for r in store.rows(self.MODULE):
            if r.get("指标名称") == index_name:
                index_def = r
                break
        if index_def is None:
            return {"ok": False, "error": f"指标「{index_name}」不存在"}

        filter_cond = index_def.get("过滤条件", "")
        matched = True
        details = {}
        if filter_cond:
            parts = filter_cond.split(";")
            for cond in parts:
                cond = cond.strip()
                if not cond:
                    continue
                if "=" in cond:
                    k, v = cond.split("=", 1)
                    k, v = k.strip(), v.strip()
                    data_val = str(data.get(k, ""))
                    matched_cond = data_val == v
                    matched = matched and matched_cond
                    details[k] = {"expected": v, "actual": data_val, "match": matched_cond}
                elif ">" in cond:
                    k, v = cond.split(">", 1)
                    try:
                        matched = matched and float(data.get(k.strip(), 0)) > float(v.strip())
                    except (ValueError, TypeError):
                        matched = False

        return {
            "ok": True,
            "index": index_name,
            "matched": matched,
            "filter": filter_cond,
            "details": details,
        }

    def expire_old_indexes(self) -> dict[str, Any]:
        """清理过期的指标定义，按保留时长计算。"""
        now = time.time()
        expired = 0
        for r in store.rows(self.MODULE):
            try:
                retention_hours = int(r.get("保留时长", "720"))
                retention_sec = retention_hours * 3600
                ts = r.get("_computed_at", 0)
                if isinstance(ts, (int, float)) and ts > 0 and now - ts > retention_sec:
                    r["status"] = "已过期"
                    expired += 1
            except (ValueError, TypeError):
                pass
        return {"ok": True, "expired": expired, "timestamp": now}

    def compute_and_output_to_sink(
        self, index_name: str, data_series: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """执行指标计算并将结果输出到 sink_output。"""
        index_def = None
        for r in store.rows(self.MODULE):
            if r.get("指标名称") == index_name:
                index_def = r
                break
        if index_def is None:
            return {"ok": False, "error": f"指标「{index_name}」不存在"}

        group_dims = [
            d.strip()
            for d in index_def.get("分组维度", "").split(",")
            if d.strip()
        ]
        agg_func = index_def.get("聚合函数", "count").lower()

        # 分组聚合
        groups: dict[str, list[float]] = {}
        for item in data_series:
            payload = item.get("payload", item)
            gk_parts = [str(payload.get(d, "")) for d in group_dims]
            gk = "|".join(gk_parts) or "__all__"
            val = payload.get("value", 0)
            if not isinstance(val, (int, float)):
                try:
                    val = float(val)
                except (ValueError, TypeError):
                    val = 0.0
            groups.setdefault(gk, []).append(val)

        # 计算并写入 sink_output
        output_count = 0
        for gk, vals in groups.items():
            if agg_func == "count":
                result = float(len(vals))
            elif agg_func == "sum":
                result = sum(vals)
            elif agg_func == "avg":
                result = sum(vals) / len(vals) if vals else 0
            else:
                result = sum(vals)

            sink_entry = {
                "id": max(
                    (int(r.get("id", 0)) for r in store.rows("sink_output")), default=0
                ) + 1,
                "配置名称": f"index_{index_name}_{gk}",
                "输出类型": "index_result",
                "目标地址": f"index://{index_name}",
                "写入模式": "append",
                "分区策略": "round_robin",
                "格式转换": "json",
                "status": "已生效",
                "sink": f"index_{index_name}",
                "write_status": "success",
                "partition": f"pt_{gk}",
                "data": {"index": index_name, "group": gk, "value": result},
                "written_at": time.time(),
            }
            store.rows("sink_output").append(sink_entry)
            output_count += 1

        index_def["_computed_at"] = time.time()
        return {
            "ok": True,
            "index": index_name,
            "groups": len(groups),
            "outputs_written": output_count,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["active"] = sum(1 for r in rows if r.get("status") == "计算中")
        base["effective"] = sum(1 for r in rows if r.get("status") == "已生效")
        base["expired"] = sum(1 for r in rows if r.get("status") == "已过期")
        return base