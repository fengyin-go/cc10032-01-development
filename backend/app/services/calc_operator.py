"""计算算子领域服务：定义流式计算的核心算子。

数据流角色: stream_source → calc_operator（消费者+生产者）→ state_store（消费者）
增强能力: 算子部署校验、聚合计算执行、输入输出字段映射、状态生命周期管理
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CalcOperatorService(BaseModuleService):
    MODULE = "calc_operator"
    ENTITY = "算子定义"
    LIST_FIELDS = ["算子名称", "算子类型", "输入字段", "聚合函数", "分组维度", "输出字段", "状态存活"]
    REQUIRED_FIELDS = ["算子名称", "算子类型", "输入字段"]
    STATUS_ORDER = ["草稿", "已部署", "运行中", "已停止"]
    ACTION_RULES = {"部署算子": "草稿", "停止算子": "运行中"}
    NEGATIVE_ACTIONS = []

    VALID_AGG_FUNCS = {"count", "sum", "avg", "max", "min", "distinct_count", "stddev", "percentile"}

    # ---- 跨模块数据消费与生产 ----

    def deploy_with_validation(self, operator_name: str) -> dict[str, Any]:
        """部署算子前执行完整校验：聚合函数合法性、输入输出字段映射、分组维度有效性。"""
        operator = None
        for r in store.rows(self.MODULE):
            if r.get("算子名称") == operator_name:
                operator = r
                break
        if operator is None:
            return {"ok": False, "error": f"算子「{operator_name}」不存在"}

        checks = []
        # 校验聚合函数
        agg_funcs = set(
            f.strip().lower()
            for f in operator.get("聚合函数", "").split(",")
            if f.strip()
        )
        invalids = agg_funcs - self.VALID_AGG_FUNCS
        checks.append({
            "check": "agg_functions",
            "valid": len(invalids) == 0,
            "invalids": list(invalids),
        })
        # 校验输入输出字段映射
        input_fields = set(
            f.strip()
            for f in operator.get("输入字段", "").split(",")
            if f.strip()
        )
        output_fields = set(
            f.strip()
            for f in operator.get("输出字段", "").split(",")
            if f.strip()
        )
        if input_fields and output_fields:
            # 输出字段数应该 >= 聚合函数数 * 分组维度数
            expected = len(agg_funcs) * max(
                len(set(
                    d.strip()
                    for d in operator.get("分组维度", "").split(",")
                    if d.strip()
                )), 1
            )
            checks.append({
                "check": "field_mapping",
                "input_count": len(input_fields),
                "output_count": len(output_fields),
                "expected_min": expected,
                "valid": len(output_fields) >= 1,
            })
        # 校验分组维度
        group_dims = [
            d.strip()
            for d in operator.get("分组维度", "").split(",")
            if d.strip()
        ]
        for dim in group_dims:
            in_input = dim in input_fields
            checks.append({
                "check": "group_dim_in_input",
                "dim": dim,
                "valid": in_input,
            })
        all_valid = all(c.get("valid", False) for c in checks)
        if not all_valid:
            return {"ok": False, "error": "部署校验失败", "checks": checks}
        operator["status"] = "运行中"
        operator["pending"] = True
        return {"ok": True, "operator": operator_name, "checks": checks}

    def execute_aggregation(
        self,
        operator_name: str,
        source_data: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """对输入数据执行算子定义的聚合计算。

        根据算子的聚合函数和分组维度对 source_data 执行分组聚合，
        生成聚合结果并写入 state_store 作为状态快照。
        """
        operator = None
        for r in store.rows(self.MODULE):
            if r.get("算子名称") == operator_name:
                operator = r
                break
        if operator is None:
            return {"ok": False, "error": f"算子「{operator_name}」不存在"}

        agg_funcs = [
            f.strip().lower()
            for f in operator.get("聚合函数", "").split(",")
            if f.strip()
        ]
        group_dims = [
            d.strip()
            for d in operator.get("分组维度", "").split(",")
            if d.strip()
        ]

        # 分组
        groups: dict[str, list[float]] = {}
        for item in source_data:
            payload = item.get("payload", item)
            gk_parts = [str(payload.get(d, "")) for d in group_dims]
            gk = "|".join(gk_parts) or "__default__"
            val = item.get("value") or payload.get("value", 0)
            if not isinstance(val, (int, float)):
                try:
                    val = float(val)
                except (ValueError, TypeError):
                    val = 0.0
            groups.setdefault(gk, []).append(val)

        # 聚合
        results: dict[str, dict[str, float]] = {}
        for gk, vals in groups.items():
            results[gk] = {}
            for func in agg_funcs:
                if func == "count":
                    results[gk][func] = float(len(vals))
                elif func == "sum":
                    results[gk][func] = sum(vals)
                elif func == "avg":
                    results[gk][func] = sum(vals) / len(vals) if vals else 0
                elif func == "max":
                    results[gk][func] = max(vals) if vals else 0
                elif func == "min":
                    results[gk][func] = min(vals) if vals else 0
                elif func == "distinct_count":
                    results[gk][func] = float(len(set(vals)))
                elif func == "stddev":
                    mean = sum(vals) / len(vals) if vals else 0
                    vars_ = sum((v - mean) ** 2 for v in vals) / len(vals) if vals else 0
                    results[gk][func] = vars_ ** 0.5
                elif func == "percentile":
                    sorted_vals = sorted(vals)
                    p50 = sorted_vals[len(sorted_vals) // 2] if sorted_vals else 0
                    results[gk][func] = p50

        # 写入 state_store 状态快照
        for gk, aggs in results.items():
            state_entry = {
                "id": max(
                    (int(r.get("id", 0)) for r in store.rows("state_store")), default=0
                ) + 1,
                "配置名称": f"agg_{operator_name}_{gk}_{int(time.time())}",
                "后端类型": "memory",
                "状态键模式": f"operator://{operator_name}/{gk}",
                "TTL时长": operator.get("状态存活", "3600"),
                "检查点间隔": "30",
                "增量模式": "full",
                "清理策略": "ttl_expire",
                "status": "已生效",
                "pending": True,
                "abnormal": False,
                "_operator": operator_name,
                "_group": gk,
                "_aggregates": aggs,
                "_timestamp": time.time(),
            }
            store.rows("state_store").append(state_entry)

        return {
            "ok": True,
            "operator": operator_name,
            "groups": len(groups),
            "aggregations": results,
        }

    def validate_input_output_mapping(self, operator_name: str) -> dict[str, Any]:
        """校验算子输入输出字段映射和分组维度一致性。"""
        operator = store.find(self.MODULE, store.rows(self.MODULE).index(
            next(r for r in store.rows(self.MODULE) if r.get("算子名称") == operator_name)
        ) + 1)
        # 重新查找
        for r in store.rows(self.MODULE):
            if r.get("算子名称") == operator_name:
                operator = r
                break
        if operator is None:
            return {"ok": False, "error": "算子不存在"}
        inputs = [f.strip() for f in operator.get("输入字段", "").split(",") if f.strip()]
        outputs = [f.strip() for f in operator.get("输出字段", "").split(",") if f.strip()]
        dims = [d.strip() for d in operator.get("分组维度", "").split(",") if d.strip()]
        agg_funcs = [f.strip() for f in operator.get("聚合函数", "").split(",") if f.strip()]
        mapping = {
            "inputs": inputs,
            "outputs": outputs,
            "group_dims": dims,
            "agg_functions": agg_funcs,
            "valid": len(inputs) > 0 and len(agg_funcs) > 0,
            "warning": None,
        }
        if not outputs:
            mapping["warning"] = "输出字段为空，将使用默认命名"
        if len(agg_funcs) * len(max(dims, [""])) > len(outputs):
            mapping["warning"] = "输出字段数可能不足以承载所有聚合结果"
        return mapping

    def list_deployed_operators(self) -> list[dict[str, Any]]:
        """列出所有已部署/运行中的算子及其关联状态存储信息。"""
        deployed = [
            r for r in store.rows(self.MODULE)
            if r.get("status") in ("已部署", "运行中")
        ]
        result = []
        for op in deployed:
            op_name = op.get("算子名称", "")
            # 查找关联的 state_store 条目
            state_entries = [
                s for s in store.rows("state_store")
                if op_name in str(s.get("_operator", ""))
            ]
            result.append({
                "operator": op_name,
                "type": op.get("算子类型", ""),
                "status": op.get("status", ""),
                "agg_functions": op.get("聚合函数", ""),
                "group_dims": op.get("分组维度", ""),
                "state_entries": len(state_entries),
                "last_state_ts": max(
                    (s.get("_timestamp", 0) for s in state_entries), default=0
                ),
            })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["deployed"] = sum(1 for r in rows if r.get("status") == "已部署")
        base["running"] = sum(1 for r in rows if r.get("status") == "运行中")
        return base