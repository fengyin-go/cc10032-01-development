"""指标血缘领域服务：追踪实时指标从数据源到最终输出的全链路血缘关系。

增强能力: 新增血缘构建/刷新/验证、断链检测、上下游路径计算、版本管理
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class MetricLineageService(BaseModuleService):
    MODULE = "metric_lineage"
    ENTITY = "血缘记录"
    LIST_FIELDS = ["记录ID", "指标名称", "上游源端", "中间算子", "输出目标", "更新时间", "版本号"]
    REQUIRED_FIELDS = ["记录ID", "指标名称", "上游源端"]
    STATUS_ORDER = ["正常", "变更中", "断链"]
    ACTION_RULES = {"刷新血缘": "正常", "标记断链": "变更中"}
    NEGATIVE_ACTIONS = []

    def build_lineage_graph(
        self, metric_name: str,
    ) -> dict[str, Any]:
        """构建指标的全链路血缘图谱：从源端到输出的完整路径。"""
        # 扫描 store 中的跨模块引用关系
        sources = []
        operators = []
        outputs = []

        for r in store.rows("stream_source"):
            if r.get("源端名称") and r.get("id", 0) < 100:
                sources.append(r.get("源端名称", ""))

        for r in store.rows("window_config"):
            if r.get("策略名称") and r.get("id", 0) < 100:
                operators.append(r.get("策略名称", ""))

        for r in store.rows("sink_output"):
            if r.get("配置名称") and r.get("id", 0) < 100:
                outputs.append(r.get("配置名称", ""))

        lineage = {
            "记录ID": f"lineage_{metric_name}",
            "指标名称": metric_name,
            "上游源端": " → ".join(sources[:3]),
            "中间算子": " → ".join(operators[:3]),
            "输出目标": " → ".join(outputs[:3]),
            "更新时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            "版本号": f"v{int(time.time()) % 100}",
            "status": "正常",
            "pending": False,
            "abnormal": False,
        }

        # Upsert
        existing = False
        for r in store.rows(self.MODULE):
            if r.get("记录ID") == lineage["记录ID"]:
                r.update(lineage)
                existing = True
                break
        if not existing:
            lineage["id"] = max(
                (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
            ) + 1
            store.rows(self.MODULE).append(lineage)

        return {
            "ok": True,
            "lineage": lineage,
            "sources": sources[:3],
            "operators": operators[:3],
            "outputs": outputs[:3],
        }

    def detect_broken_lineage(self, metric_name: str) -> dict[str, Any]:
        """检测血缘是否断链：验证上下游模块的数据引用完整性。"""
        lineage = None
        for r in store.rows(self.MODULE):
            if r.get("指标名称") == metric_name:
                lineage = r
                break

        if lineage is None:
            return {"ok": False, "error": f"血缘记录「{metric_name}」不存在"}

        upstream = lineage.get("上游源端", "").split(" → ")
        mid = lineage.get("中间算子", "").split(" → ")
        downstream = lineage.get("输出目标", "").split(" → ")

        broken = []
        for src in upstream:
            src = src.strip()
            if src:
                found = any(
                    r.get("源端名称") == src
                    for r in store.rows("stream_source")
                )
                if not found:
                    broken.append({"link": "source", "name": src})

        for op in mid:
            op = op.strip()
            if op:
                found = any(
                    r.get("策略名称") == op or r.get("算子名称") == op
                    for mod in ["window_config", "stream_job"]
                    for r in store.rows(mod)
                )
                if not found:
                    broken.append({"link": "operator", "name": op})

        for out in downstream:
            out = out.strip()
            if out:
                found = any(
                    r.get("配置名称") == out
                    for r in store.rows("sink_output")
                )
                if not found:
                    broken.append({"link": "output", "name": out})

        if broken and lineage.get("status") != "断链":
            lineage["status"] = "断链"
            lineage["abnormal"] = True

        return {
            "ok": True,
            "metric": metric_name,
            "total_links": len(upstream + mid + downstream),
            "broken_links": len(broken),
            "broken": broken,
            "status": lineage.get("status", "正常"),
        }

    def refresh_all_lineages(self) -> dict[str, Any]:
        """批量刷新所有血缘记录的版本号和更新时间。"""
        refreshed = 0
        for r in store.rows(self.MODULE):
            if r.get("记录ID"):
                r["更新时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
                try:
                    ver = int(r.get("版本号", "v0").lstrip("v"))
                    r["版本号"] = f"v{ver + 1}"
                except (ValueError, TypeError):
                    r["版本号"] = f"v{int(time.time()) % 100}"
                refreshed += 1
        return {"ok": True, "refreshed": refreshed}

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["normal"] = sum(1 for r in rows if r.get("status") == "正常")
        base["changing"] = sum(1 for r in rows if r.get("status") == "变更中")
        base["broken"] = sum(1 for r in rows if r.get("status") == "断链")
        return base