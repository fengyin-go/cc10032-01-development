"""补数任务领域服务：管理实时指标的离线补数任务。

数据流角色: watermark_track / late_handler → backfill_job（消费者）
增强能力: 补数进度跟踪、范围校验、并行度优化、去重、异常补数、与聚合落库联动验证
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class BackfillJobService(BaseModuleService):
    MODULE = "backfill_job"
    ENTITY = "补数作业"
    LIST_FIELDS = ["作业名称", "目标指标", "补数范围", "并行度", "起始位点", "截止位点", "进度比例"]
    REQUIRED_FIELDS = ["作业名称", "目标指标", "补数范围"]
    STATUS_ORDER = ["待执行", "补数中", "已完成", "部分失败"]
    ACTION_RULES = {"启动补数": "待执行", "暂停补数": "补数中"}
    NEGATIVE_ACTIONS = []

    # ---- 跨模块数据管理 ----

    def validate_backfill_range(
        self, target_metric: str, start_offset: str, end_offset: str,
    ) -> dict[str, Any]:
        """校验补数范围的合法性：起止位点不能倒置、范围不能为0。"""
        issues = []
        try:
            start = int(start_offset)
        except (ValueError, TypeError):
            issues.append("起始位点必须为整数")
            start = 0
        try:
            end = int(end_offset)
        except (ValueError, TypeError):
            issues.append("截止位点必须为整数")
            end = 0

        if start > end:
            issues.append("起始位点不能大于截止位点")
        if start == end:
            issues.append("补数范围不能为空（起始位点等于截止位点）")

        # 检查是否与已有任务重复
        for r in store.rows(self.MODULE):
            if r.get("目标指标") == target_metric and r.get("status") in ("补数中",):
                existing_start = str(r.get("起始位点", ""))
                existing_end = str(r.get("截止位点", ""))
                if existing_start == start_offset and existing_end == end_offset:
                    issues.append(f"指标「{target_metric}」已有补数任务「{r.get('作业名称')}」在处理相同范围")

        return {
            "ok": len(issues) == 0,
            "target": target_metric,
            "range": f"{start_offset}-{end_offset}",
            "range_size": end - start if isinstance(start, int) and isinstance(end, int) else 0,
            "issues": issues,
        }

    def estimate_parallelism(self, range_size: int, data_rate: float = 1000.0) -> dict[str, Any]:
        """根据数据范围自动推算最优并行度。"""
        # 每个分区建议处理 10000 条，最少 1 并行
        optimal = max(1, min(32, (range_size * data_rate) // 10000))
        return {
            "range_size": range_size,
            "estimated_events": int(range_size * data_rate),
            "recommended_parallelism": optimal,
            "estimated_duration_sec": (range_size * data_rate) / (optimal * 100),
        }

    def track_progress(self, job_name: str) -> dict[str, Any]:
        """跟踪补数任务进度并计算完成百分比。"""
        job = None
        for r in store.rows(self.MODULE):
            if r.get("作业名称") == job_name:
                job = r
                break
        if job is None:
            return {"ok": False, "error": f"补数任务「{job_name}」不存在"}

        try:
            start = int(job.get("起始位点", "0"))
            end = int(job.get("截止位点", "0"))
            current = int(job.get("进度比例", "0").replace("%", ""))
        except (ValueError, TypeError):
            start, end, current = 0, 100, 0

        total_range = max(end - start, 1)
        progress_pct = min(100, max(0, current))

        # 模拟进度推进
        if job.get("status") == "补数中":
            progress_pct = min(100, current + 15)
            job["进度比例"] = f"{progress_pct}%"
            if progress_pct >= 100:
                job["status"] = "已完成"
                job["pending"] = False

                # 验证补数结果：与聚合落库对比
                self._verify_with_aggregate_sink(job)

        return {
            "ok": True,
            "job": job_name,
            "status": job.get("status", ""),
            "progress_pct": progress_pct,
            "total_range": total_range,
            "completed_range": int(total_range * progress_pct / 100),
        }

    def _verify_with_aggregate_sink(self, job: dict[str, Any]) -> None:
        """补数完成后与聚合落库进行交叉验证。"""
        target = job.get("目标指标", "")
        matched = sum(
            1 for r in store.rows("aggregate_sink")
            if target in str(r.get("源指标集", ""))
        )
        if matched > 0:
            job["_verified_with_aggregate"] = matched
            job["_verified_at"] = time.time()

    def deduplicate_backfill_jobs(self) -> dict[str, Any]:
        """去重：移除目标指标相同的重复补数任务，保留最新。"""
        grouped: dict[str, list[dict[str, Any]]] = {}
        for r in store.rows(self.MODULE):
            metric = r.get("目标指标", "")
            if metric and r.get("id", 0) < 1000:
                grouped.setdefault(metric, []).append(r)

        deduplicated = 0
        for metric, jobs in grouped.items():
            if len(jobs) > 1:
                jobs.sort(key=lambda j: j.get("id", 0), reverse=True)
                for j in jobs[1:]:
                    if j.get("status") in ("待执行",):
                        j["status"] = "已完成"
                        j["_deduplicated"] = True
                        deduplicated += 1

        return {
            "ok": True,
            "metrics_with_duplicates": sum(1 for jobs in grouped.values() if len(jobs) > 1),
            "deduplicated_jobs": deduplicated,
        }

    def list_pending_backfills(self) -> list[dict[str, Any]]:
        """列出所有待执行和补数中的任务。"""
        pending = [
            r for r in store.rows(self.MODULE)
            if r.get("status") in ("待执行", "补数中")
        ]
        result = []
        for j in pending:
            entry = {
                "job_id": j.get("id"),
                "job_name": j.get("作业名称", ""),
                "target": j.get("目标指标", ""),
                "range": j.get("补数范围", ""),
                "parallelism": j.get("并行度", ""),
                "progress": j.get("进度比例", "0%"),
                "status": j.get("status", ""),
            }
            # 检查水位追踪关联
            for wt in store.rows("watermark_track"):
                if wt.get("数据源") in str(j.get("目标指标", "")):
                    entry["watermark_status"] = wt.get("status", "")
                    break
            result.append(entry)
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["pending_jobs"] = sum(1 for r in rows if r.get("status") == "待执行")
        base["running_jobs"] = sum(1 for r in rows if r.get("status") == "补数中")
        base["completed_jobs"] = sum(1 for r in rows if r.get("status") == "已完成")
        base["partial_failures"] = sum(1 for r in rows if r.get("status") == "部分失败")
        base["deduplicated"] = sum(1 for r in rows if r.get("_deduplicated"))
        return base