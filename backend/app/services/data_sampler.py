"""数据采样领域服务：管理流式数据的采样策略。

增强能力: 采样策略应用、样本特征分析、分层采样、样本保留校验、输出到debug_trace
"""
from __future__ import annotations

import random
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DataSamplerService(BaseModuleService):
    MODULE = "data_sampler"
    ENTITY = "采样策略"
    LIST_FIELDS = ["策略名称", "采样方式", "采样比例", "分组键", "保留特征", "样本上限", "采样窗口"]
    REQUIRED_FIELDS = ["策略名称", "采样方式", "采样比例"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用策略": "草稿", "停用策略": "已启用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    def apply_sampling(
        self, sampler_name: str, data: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """对数据执行采样策略：随机采样或分层采样。"""
        sampler = None
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == sampler_name:
                sampler = r
                break
        if sampler is None:
            return {"ok": False, "error": f"采样策略「{sampler_name}」不存在"}

        method = sampler.get("采样方式", "random")
        try:
            ratio = float(sampler.get("采样比例", "0.1"))
        except (ValueError, TypeError):
            ratio = 0.1

        try:
            max_samples = int(sampler.get("样本上限", "100"))
        except (ValueError, TypeError):
            max_samples = 100

        group_key = sampler.get("分组键", "")
        features = [f.strip() for f in sampler.get("保留特征", "").split(",") if f.strip()]

        if method == "stratified" and group_key:
            sampled = self._stratified_sample(data, group_key, ratio, max_samples)
        else:
            sample_count = min(max(1, int(len(data) * ratio)), max_samples)
            random.seed(42)
            sampled = random.sample(data, min(sample_count, len(data)))

        # 保存采样结果
        result_entry = {
            "id": max(
                (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
            ) + 1,
            "策略名称": sampler_name,
            "采样方式": method,
            "采样比例": str(ratio),
            "分组键": group_key,
            "保留特征": ",".join(features),
            "样本上限": str(max_samples),
            "采样窗口": sampler.get("采样窗口", "60"),
            "status": "已启用",
            "pending": True,
            "abnormal": False,
            "_sampled_data": sampled,
            "_sample_count": len(sampled),
            "_total_count": len(data),
            "_sampled_at": time.time(),
        }
        store.rows(self.MODULE).append(result_entry)

        return {
            "ok": True,
            "sampler": sampler_name,
            "method": method,
            "total": len(data),
            "sampled": len(sampled),
            "ratio": round(len(sampled) / max(len(data), 1), 4),
        }

    @staticmethod
    def _stratified_sample(
        data: list[dict[str, Any]], group_key: str, ratio: float, max_total: int,
    ) -> list[dict[str, Any]]:
        """分层采样：按分组键分层后比例抽取。"""
        strata: dict[str, list[dict[str, Any]]] = {}
        for item in data:
            key = str(item.get("payload", {}).get(group_key, item.get(group_key, "__default__")))
            strata.setdefault(key, []).append(item)
        sampled = []
        for items in strata.values():
            count = max(1, int(len(items) * ratio))
            random.seed(42)
            sampled.extend(random.sample(items, min(count, len(items))))
        random.shuffle(sampled)
        return sampled[:max_total]

    def compute_sample_features(
        self, sampler_name: str, sampled_data: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """分析采样数据的统计特征：分布、均值、方差等。"""
        if not sampled_data:
            return {"ok": True, "sampler": sampler_name, "features": {}}

        values = []
        for item in sampled_data:
            payload = item.get("payload", item)
            val = payload.get("value", 0)
            if not isinstance(val, (int, float)):
                try:
                    val = float(val)
                except (ValueError, TypeError):
                    val = 0.0
            values.append(val)

        n = len(values)
        mean = sum(values) / n
        variance = sum((v - mean) ** 2 for v in values) / n
        stddev = variance ** 0.5
        sorted_vals = sorted(values)

        features = {
            "count": n,
            "mean": round(mean, 4),
            "stddev": round(stddev, 4),
            "min": sorted_vals[0] if sorted_vals else 0,
            "max": sorted_vals[-1] if sorted_vals else 0,
            "p50": sorted_vals[n // 2] if n > 0 else 0,
            "p95": sorted_vals[int(n * 0.95)] if n > 1 else (sorted_vals[0] if n > 0 else 0),
        }
        return {"ok": True, "sampler": sampler_name, "features": features}

    def list_threshold_breaches(self, sampler_name: str) -> list[dict[str, Any]]:
        """列出超出样本上限的采样策略。"""
        breaches = []
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == sampler_name:
                break
        else:
            return breaches

        for r in store.rows(self.MODULE):
            if r.get("策略名称") == sampler_name and r.get("_sample_count"):
                try:
                    max_samples = int(r.get("样本上限", "100"))
                except (ValueError, TypeError):
                    max_samples = 100
                if r["_sample_count"] > max_samples:
                    breaches.append({
                        "sampler": r.get("策略名称", ""),
                        "sample_count": r["_sample_count"],
                        "limit": max_samples,
                        "exceed_by": r["_sample_count"] - max_samples,
                    })
        return breaches

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["total_samples"] = sum(
            r.get("_sample_count", 0) for r in rows if r.get("_sample_count")
        )
        return base