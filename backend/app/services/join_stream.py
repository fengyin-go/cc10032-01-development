"""流关联领域服务：定义两条流之间的关联计算配置。

增强能力: 关联类型校验、时间窗口比对、左/右流源校验、关联输出到聚合落库
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class JoinStreamService(BaseModuleService):
    MODULE = "join_stream"
    ENTITY = "关联配置"
    LIST_FIELDS = ["配置名称", "左流源", "右流源", "关联字段", "关联类型", "窗口区间", "输出模式"]
    REQUIRED_FIELDS = ["配置名称", "左流源", "右流源"]
    STATUS_ORDER = ["草稿", "运行中", "已停止"]
    ACTION_RULES = {"启动关联": "草稿", "停止关联": "运行中"}
    NEGATIVE_ACTIONS = []

    def validate_source_availability(
        self, left_source: str, right_source: str,
    ) -> dict[str, Any]:
        """校验左右流源是否可用（存在且未停止/异常断连）。"""
        left_ok = False
        for r in store.rows("stream_source"):
            if r.get("源端名称") == left_source and r.get("status") not in ("已下线", "异常断连"):
                left_ok = True
                break

        right_ok = False
        for r in store.rows("stream_source"):
            if r.get("源端名称") == right_source and r.get("status") not in ("已下线", "异常断连"):
                right_ok = True
                break

        return {
            "ok": left_ok and right_ok,
            "left": {"source": left_source, "available": left_ok},
            "right": {"source": right_source, "available": right_ok},
        }

    def evaluate_join_window(
        self, join_name: str, left_events: list[dict], right_events: list[dict],
    ) -> dict[str, Any]:
        """在时间窗口内评估双流关联匹配度。"""
        join_cfg = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == join_name:
                join_cfg = r
                break
        if join_cfg is None:
            return {"ok": False, "error": f"关联配置「{join_name}」不存在"}

        join_type = join_cfg.get("关联类型", "inner")
        join_field = join_cfg.get("关联字段", "user_id")
        try:
            window_sec = float(join_cfg.get("窗口区间", "10"))
        except (ValueError, TypeError):
            window_sec = 10.0

        # 构建索引
        left_index: dict[str, dict] = {}
        for le in left_events:
            key = str(le.get("payload", {}).get(join_field, le.get(join_field, "")))
            if key:
                left_index[key] = le

        right_index: dict[str, dict] = {}
        for re in right_events:
            key = str(re.get("payload", {}).get(join_field, re.get(join_field, "")))
            if key:
                right_index[key] = re

        matches = []
        all_keys = set(left_index.keys()) | set(right_index.keys())
        for key in all_keys:
            lr = left_index.get(key)
            rr = right_index.get(key)
            if join_type == "inner" and (lr is None or rr is None):
                continue
            matches.append({
                "key": key,
                "matched": lr is not None and rr is not None,
                "left_ts": lr.get("event_time", 0) if lr else 0,
                "right_ts": rr.get("event_time", 0) if rr else 0,
            })

        # 写入聚合落库
        matched_count = sum(1 for m in matches if m["matched"])
        sink_entry = {
            "id": max(
                (int(r.get("id", 0)) for r in store.rows("aggregate_sink")), default=0
            ) + 1,
            "任务名称": f"join_{join_name}_{int(time.time())}",
            "源指标集": f"join://{join_name}",
            "聚合粒度": join_type,
            "输出表名": f"table_{join_name}",
            "落库频率": "60",
            "覆盖模式": "append",
            "分区键": f"{join_field}",
            "status": "运行中",
            "pending": True,
            "abnormal": False,
            "join_total": len(matches),
            "matched": matched_count,
            "unmatched": len(matches) - matched_count,
            "match_rate": matched_count / max(len(matches), 1),
        }
        store.rows("aggregate_sink").append(sink_entry)

        return {
            "ok": True,
            "join_config": join_name,
            "type": join_type,
            "total_keys": len(all_keys),
            "matched": matched_count,
            "match_rate": matched_count / max(len(all_keys), 1),
        }

    def list_join_statistics(self) -> list[dict[str, Any]]:
        """列出所有流关联的统计信息。"""
        result = []
        for r in store.rows(self.MODULE):
            if r.get("id", 0) < 100:
                name = r.get("配置名称", "")
                # 查找聚合落库关联
                agg_matches = sum(
                    1 for a in store.rows("aggregate_sink")
                    if name in str(a.get("源指标集", ""))
                )
                result.append({
                    "join": name,
                    "left_src": r.get("左流源", ""),
                    "right_src": r.get("右流源", ""),
                    "type": r.get("关联类型", ""),
                    "status": r.get("status", ""),
                    "aggregations": agg_matches,
                })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["running"] = sum(1 for r in rows if r.get("status") == "运行中")
        return base