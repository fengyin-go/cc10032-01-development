"""状态存储领域服务：管理流式任务的有状态存储配置。

数据流角色: calc_operator → state_store（消费者+生产者）→ 下游多模块
增强能力: 状态TTL清理、增量/全量模式切换、状态键去重、状态查询、跨模块状态索引
"""
from __future__ import annotations

import copy
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class StateStoreService(BaseModuleService):
    MODULE = "state_store"
    ENTITY = "状态配置"
    LIST_FIELDS = ["配置名称", "后端类型", "状态键模式", "TTL时长", "检查点间隔", "增量模式", "清理策略"]
    REQUIRED_FIELDS = ["配置名称", "后端类型", "状态键模式"]
    STATUS_ORDER = ["草稿", "已生效", "已停用"]
    ACTION_RULES = {"启用配置": "草稿", "停用配置": "已生效"}
    NEGATIVE_ACTIONS = ["停用配置"]

    # ---- 跨模块数据管理 ----

    def compact_duplicate_states(self, config_name: str) -> dict[str, Any]:
        """压缩重复状态：按状态键模式去重，保留最新版本。"""
        pattern = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == config_name:
                pattern = r.get("状态键模式", "")
                break
        if not pattern:
            return {"ok": False, "error": f"状态配置「{config_name}」不存在"}

        states_grouped: dict[str, list[dict[str, Any]]] = {}
        for r in store.rows(self.MODULE):
            if r.get("状态键模式") and pattern in str(r.get("状态键模式", "")):
                key = r.get("状态键模式", "__default__")
                states_grouped.setdefault(key, []).append(r)

        compacted = 0
        for key, entries in states_grouped.items():
            if len(entries) > 1:
                # 保留最新的，标记其余为已停用
                entries.sort(key=lambda e: e.get("_timestamp", 0), reverse=True)
                for e in entries[1:]:
                    e["status"] = "已停用"
                    e["_compacted"] = True
                    compacted += 1

        return {
            "ok": True,
            "config": config_name,
            "pattern": pattern,
            "groups_found": len(states_grouped),
            "compacted": compacted,
            "remaining_active": sum(
                len(entries) for entries in states_grouped.values()
            ) - compacted,
        }

    def apply_ttl_cleanup(self, config_name: str) -> dict[str, Any]:
        """根据TTL策略清理过期状态条目。"""
        config = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == config_name:
                config = r
                break
        if config is None:
            return {"ok": False, "error": f"状态配置「{config_name}」不存在"}

        try:
            ttl = int(config.get("TTL时长", "3600"))
        except (ValueError, TypeError):
            ttl = 3600

        now = time.time()
        cleaned = 0
        for r in store.rows(self.MODULE):
            ts = r.get("_timestamp", 0)
            if isinstance(ts, (int, float)) and now - ts > ttl:
                r["status"] = "已停用"
                r["_cleaned"] = True
                r["_cleaned_at"] = now
                cleaned += 1

        return {
            "ok": True,
            "config": config_name,
            "ttl": ttl,
            "cleaned": cleaned,
            "timestamp": now,
        }

    def query_state_by_operator(
        self, operator_name: str, group_key: str | None = None,
    ) -> dict[str, Any]:
        """按算子名称查询关联状态，支持分组过滤。"""
        matches = []
        for r in store.rows(self.MODULE):
            if r.get("_operator") == operator_name:
                if group_key and r.get("_group") != group_key:
                    continue
                matches.append({
                    "state_id": r.get("id"),
                    "config": r.get("配置名称", ""),
                    "operator": r.get("_operator", ""),
                    "group": r.get("_group", ""),
                    "aggregates": r.get("_aggregates", {}),
                    "timestamp": r.get("_timestamp", 0),
                    "status": r.get("status", ""),
                })
        return {
            "ok": True,
            "operator": operator_name,
            "group_filter": group_key,
            "count": len(matches),
            "states": matches,
        }

    def list_active_state_keys(self) -> list[dict[str, Any]]:
        """列出所有活跃的状态键及其数据量。"""
        key_stats: dict[str, dict[str, Any]] = {}
        for r in store.rows(self.MODULE):
            if r.get("status") in ("已生效",):
                pattern = r.get("状态键模式", "__unknown__")
                if pattern not in key_stats:
                    key_stats[pattern] = {"pattern": pattern, "count": 0, "latest_ts": 0}
                key_stats[pattern]["count"] += 1
                ts = r.get("_timestamp", 0)
                if isinstance(ts, (int, float)) and ts > key_stats[pattern]["latest_ts"]:
                    key_stats[pattern]["latest_ts"] = ts
        return sorted(key_stats.values(), key=lambda x: x["count"], reverse=True)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["active_states"] = sum(1 for r in rows if r.get("status") == "已生效")
        base["compacted_states"] = sum(1 for r in rows if r.get("_compacted"))
        base["cleaned_states"] = sum(1 for r in rows if r.get("_cleaned"))
        base["unique_key_patterns"] = len(set(
            r.get("状态键模式", "") for r in rows
        ))
        return base