"""任务拓扑领域服务：管理流式计算任务的DAG拓扑结构。

增强能力: 算子链校验、节点分发策略、故障转移评估、资源配额检查、关联stream_job
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class TaskTopologyService(BaseModuleService):
    MODULE = "task_topology"
    ENTITY = "拓扑配置"
    LIST_FIELDS = ["拓扑名称", "算子链", "并行度", "数据分发", "故障转移", "资源配额", "运行状态"]
    REQUIRED_FIELDS = ["拓扑名称", "算子链", "并行度"]
    STATUS_ORDER = ["草稿", "已部署", "运行中", "已停止"]
    ACTION_RULES = {"部署拓扑": "草稿", "停止拓扑": "运行中"}
    NEGATIVE_ACTIONS = []

    def validate_topology_dag(self, topology_name: str) -> dict[str, Any]:
        """校验拓扑DAG的合法性：算子链节点数、并行度范围、资源配额。"""
        topo = None
        for r in store.rows(self.MODULE):
            if r.get("拓扑名称") == topology_name:
                topo = r
                break
        if topo is None:
            return {"ok": False, "error": f"拓扑「{topology_name}」不存在"}

        checks = []
        chain = [
            c.strip()
            for c in topo.get("算子链", "").split("→")
            if c.strip()
        ]
        checks.append({
            "check": "chain_nodes",
            "ok": 2 <= len(chain) <= 10,
            "detail": f"算子链节点数: {len(chain)}",
        })

        try:
            parallelism = int(topo.get("并行度", "4"))
            checks.append({
                "check": "parallelism",
                "ok": 1 <= parallelism <= 64,
                "detail": f"并行度: {parallelism}",
            })
        except (ValueError, TypeError):
            checks.append({"check": "parallelism", "ok": False, "detail": "并行度格式错误"})

        # 资源配额校验
        quota = topo.get("资源配额", "")
        if quota:
            checks.append({
                "check": "resource_quota",
                "ok": any(unit in quota for unit in ("cpu", "mem", "gpu", "core")),
                "detail": f"资源配额: {quota}",
            })

        return {
            "ok": all(c["ok"] for c in checks),
            "topology": topology_name,
            "chain": chain,
            "checks": checks,
        }

    def deploy_with_topology_check(
        self, topology_name: str,
    ) -> dict[str, Any]:
        """部署拓扑前检查算子链中每个节点对应的模块是否可用。"""
        topo = None
        for r in store.rows(self.MODULE):
            if r.get("拓扑名称") == topology_name:
                topo = r
                break
        if topo is None:
            return {"ok": False, "error": f"拓扑「{topology_name}」不存在"}

        chain = [
            c.strip()
            for c in topo.get("算子链", "").split("→")
            if c.strip()
        ]
        node_status = []
        for node in chain:
            found = False
            for mod in ["stream_source", "calc_operator", "sliding_index", "window_config"]:
                for r in store.rows(mod):
                    if node in str(r.get(list(r.keys())[:3])):
                        found = True
                        node_status.append({
                            "node": node,
                            "module": mod,
                            "found": True,
                            "status": r.get("status", ""),
                        })
                        break
                if found:
                    break
            if not found:
                node_status.append({"node": node, "found": False})

        all_found = all(ns["found"] for ns in node_status)
        if all_found:
            topo["status"] = "运行中"
            topo["pending"] = True

        return {
            "ok": all_found,
            "topology": topology_name,
            "nodes": node_status,
        }

    def evaluate_failover(self, topology_name: str) -> dict[str, Any]:
        """评估故障转移能力：检查拓扑中是否有备用调度路径。"""
        topo = None
        for r in store.rows(self.MODULE):
            if r.get("拓扑名称") == topology_name:
                topo = r
                break
        if topo is None:
            return {"ok": False, "error": f"拓扑「{topology_name}」不存在"}

        failover_strategy = topo.get("故障转移", "")
        restore_plans = [
            rp for rp in store.rows("restore_plan")
            if topology_name in str(rp.get("任务范围", ""))
        ]
        checkpoints = [
            cp for cp in store.rows("checkpoint_mgr")
            if cp.get("checkpoint_id") and cp.get("status") == "已启用"
        ]

        can_failover = bool(
            failover_strategy or len(restore_plans) > 0 or len(checkpoints) > 0
        )
        return {
            "ok": True,
            "topology": topology_name,
            "can_failover": can_failover,
            "strategy": failover_strategy,
            "restore_plans": len(restore_plans),
            "available_checkpoints": len(checkpoints),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["deployed"] = sum(1 for r in rows if r.get("status") == "已部署")
        base["running"] = sum(1 for r in rows if r.get("status") == "运行中")
        return base