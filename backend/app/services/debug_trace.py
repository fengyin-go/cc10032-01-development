"""调试追踪领域服务：针对特定算子启动调试会话。

增强能力: 异常标记分析、中间结果检查、会话持续时间统计、关联采样数据分析
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DebugTraceService(BaseModuleService):
    MODULE = "debug_trace"
    ENTITY = "调试会话"
    LIST_FIELDS = ["会话名称", "目标算子", "采样数据", "中间结果", "异常标记", "开始时间", "结束时间"]
    REQUIRED_FIELDS = ["会话名称", "目标算子", "采样数据"]
    STATUS_ORDER = ["运行中", "已完成", "已终止"]
    ACTION_RULES = {"终止会话": "运行中"}
    NEGATIVE_ACTIONS = []

    def analyze_anomaly_patterns(
        self, session_name: str,
    ) -> dict[str, Any]:
        """分析调试会话中的异常标记模式。"""
        session = None
        for r in store.rows(self.MODULE):
            if r.get("会话名称") == session_name:
                session = r
                break
        if session is None:
            return {"ok": False, "error": f"调试会话「{session_name}」不存在"}

        anomaly_mark = session.get("异常标记", "none")
        intermediate = session.get("中间结果", "")
        sampled = session.get("采样数据", "")

        patterns = {
            "anomaly": anomaly_mark,
            "intermediate_count": intermediate,
            "sampled_data_ref": sampled,
            "duration": 0,
        }

        try:
            start = session.get("开始时间", "")
            end = session.get("结束时间", "")
            if start and end:
                patterns["duration"] = f"{start} → {end}"
        except Exception:
            pass

        # 分析关联数据
        target_operator = session.get("目标算子", "")
        linked_states = [
            s for s in store.rows("state_store")
            if target_operator in str(s.get("_operator", ""))
        ]
        patterns["linked_state_entries"] = len(linked_states)

        return {
            "ok": True,
            "session": session_name,
            "target": target_operator,
            "patterns": patterns,
            "status": session.get("status", ""),
        }

    def complete_session_with_analysis(
        self, session_name: str,
    ) -> dict[str, Any]:
        """完成调试会话并生成分析报告。"""
        session = None
        for r in store.rows(self.MODULE):
            if r.get("会话名称") == session_name:
                session = r
                break
        if session is None:
            return {"ok": False, "error": f"调试会话「{session_name}」不存在"}

        if session.get("status") not in ("运行中",):
            return {"ok": False, "error": f"会话状态为「{session.get('status')}」, 不可完成"}

        end_time = time.strftime("%Y-%m-%d %H:%M:%S")

        # 分析采样数据和中间结果
        sampled = session.get("采样数据", "")
        intermediate = session.get("中间结果", "")
        anomaly = "none"

        if not sampled:
            anomaly = "no_sampled_data"
        elif not intermediate or intermediate == "0":
            anomaly = "no_intermediate_results"

        session["结束时间"] = end_time
        session["异常标记"] = anomaly
        session["status"] = "已完成"
        session["pending"] = False

        analysis = {
            "target": session.get("目标算子", ""),
            "sampled_data_present": bool(sampled),
            "intermediate_count": intermediate,
            "anomaly_detected": anomaly != "none",
            "anomaly_type": anomaly,
            "completed_at": end_time,
        }

        return {"ok": True, "session": session_name, "analysis": analysis}

    def list_session_statistics(self) -> dict[str, Any]:
        """列出所有调试会话的统计信息。"""
        sessions = [
            r for r in store.rows(self.MODULE)
            if r.get("id", 0) < 1000
        ]
        running = sum(1 for s in sessions if s.get("status") == "运行中")
        completed = sum(1 for s in sessions if s.get("status") == "已完成")
        terminated = sum(1 for s in sessions if s.get("status") == "已终止")
        anomalous = sum(1 for s in sessions if s.get("异常标记") != "none")

        return {
            "total": len(sessions),
            "running": running,
            "completed": completed,
            "terminated": terminated,
            "with_anomalies": anomalous,
            "anomaly_rate": anomalous / max(len(sessions), 1),
        }

    def link_to_sampler(self, session_name: str) -> dict[str, Any]:
        """将调试会话关联到采样策略。"""
        session = None
        for r in store.rows(self.MODULE):
            if r.get("会话名称") == session_name:
                session = r
                break
        if session is None:
            return {"ok": False, "error": f"调试会话「{session_name}」不存在"}

        target = session.get("目标算子", "")
        linked = []
        for r in store.rows("data_sampler"):
            if r.get("_sample_count") and target in str(r.get("策略名称", "")):
                linked.append({
                    "sampler": r.get("策略名称", ""),
                    "sample_count": r.get("_sample_count", 0),
                })
        session["_linked_samplers"] = linked

        return {
            "ok": True,
            "session": session_name,
            "linked_samplers": len(linked),
            "details": linked,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["running_sessions"] = sum(1 for r in rows if r.get("status") == "运行中")
        base["completed_sessions"] = sum(1 for r in rows if r.get("status") == "已完成")
        return base