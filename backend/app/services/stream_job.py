"""流式作业领域服务：消费 stream_source + window_config 创建计算作业。

数据流: stream_source（源端配置）→ window_config（窗口策略）→ stream_job（流式作业）
核心逻辑: 组装源端连接、窗口策略为可调度的计算作业，支持启动、暂停、重启和状态校验。
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class StreamJobService(BaseModuleService):
    MODULE = "stream_job"
    ENTITY = "流式作业"
    LIST_FIELDS = ["作业名称", "源端配置", "窗口策略", "计算拓扑", "并行度", "启动时间", "运行水位"]
    REQUIRED_FIELDS = ["作业名称", "源端配置", "窗口策略"]
    STATUS_ORDER = ["草稿", "待启动", "运行中", "已暂停", "已停止"]
    ACTION_RULES = {"启动作业": "待启动", "暂停作业": "运行中", "停止作业": "已暂停"}
    NEGATIVE_ACTIONS = ["停止作业"]

    def assemble_job(
        self,
        source_name: str,
        window_name: str,
        job_name: str | None = None,
        parallelism: int = 4,
    ) -> dict[str, Any]:
        """从 stream_source 和 window_config 组装流式作业。

        消费 stream_source 的源端连接信息和 window_config 的窗口策略，
        校验两者引用完整性后生成流式作业记录。
        """
        # 1. 查找源端配置
        source_row = None
        for r in store.rows("stream_source"):
            if r.get("源端名称") == source_name:
                source_row = r
                break
        if source_row is None:
            return {"ok": False, "error": f"源端配置「{source_name}」不存在"}

        # 2. 查找窗口策略
        window_row = None
        for r in store.rows("window_config"):
            if r.get("策略名称") == window_name:
                window_row = r
                break
        if window_row is None:
            return {"ok": False, "error": f"窗口策略「{window_name}」不存在"}

        # 3. 校验引用完整性
        issues = []
        if source_row.get("status") == "异常断连":
            issues.append(f"源端「{source_name}」处于异常断连状态")
        if window_row.get("status") == "已停用":
            issues.append(f"窗口策略「{window_name}」已停用")
        if issues:
            return {"ok": False, "error": "; ".join(issues)}

        # 4. 生成作业记录
        name = job_name or f"{source_name}_{window_name}_{int(time.time())}"
        job = {
            "id": max(
                (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
            ) + 1,
            "作业名称": name,
            "源端配置": source_name,
            "窗口策略": window_name,
            "计算拓扑": f"topo_{name}",
            "并行度": str(parallelism),
            "启动时间": "",
            "运行水位": "0",
            "status": "草稿",
            "pending": True,
            "abnormal": False,
            "_source_topic": source_row.get("Topic名称", ""),
            "_window_type": window_row.get("窗口类型", ""),
            "_window_size": window_row.get("窗口大小", ""),
            "_created_at": time.time(),
        }
        store.rows(self.MODULE).append(job)
        return {
            "ok": True,
            "job_id": job["id"],
            "job_name": name,
            "source": source_name,
            "window": window_name,
            "parallelism": parallelism,
        }

    def validate_job_integrity(self, job_id: int) -> dict[str, Any]:
        """校验作业中源端和窗口策略的引用完整性。

        遍历作业的源端配置和窗口策略字段，检查对应的模块行是否仍然存在
        且处于可用状态，发现断链则标记为异常。
        """
        job = store.find(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "error": "作业不存在"}
        source = job.get("源端配置", "")
        window = job.get("窗口策略", "")
        refs = []
        # 源端校验
        src_found = any(
            r.get("源端名称") == source and r.get("status") != "异常断连"
            for r in store.rows("stream_source")
        )
        refs.append({"ref": "stream_source", "value": source, "valid": src_found})
        # 窗口策略校验
        win_found = any(
            r.get("策略名称") == window and r.get("status") != "已停用"
            for r in store.rows("window_config")
        )
        refs.append({"ref": "window_config", "value": window, "valid": win_found})
        all_valid = all(r["valid"] for r in refs)
        if not all_valid:
            job["abnormal"] = True
            job["_integrity_check"] = refs
        return {"ok": all_valid, "job_id": job_id, "refs": refs, "all_valid": all_valid}

    def list_running_jobs(self) -> list[dict[str, Any]]:
        """列出所有运行中的作业及其吞吐快照。"""
        running = [
            j for j in store.rows(self.MODULE)
            if j.get("status") == "运行中"
        ]
        snapshots = []
        for j in running:
            snap = {
                "job_id": j["id"],
                "job_name": j.get("作业名称", ""),
                "source": j.get("源端配置", ""),
                "window": j.get("窗口策略", ""),
                "watermark": j.get("运行水位", "0"),
            }
            # 获取对应吞吐配置
            for tp in store.rows("throughput_mgr"):
                if tp.get("数据源") == j.get("源端配置"):
                    snap["max_qps"] = tp.get("最大QPS", "N/A")
                    snap["is_throttled"] = tp.get("status") == "已触发"
                    break
            snapshots.append(snap)
        return snapshots

    def domain_snapshot(self) -> dict[str, Any]:
        """输出模块级数据质量快照，附加跨模块引用健康度。"""
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        running = sum(1 for j in rows if j.get("status") == "运行中")
        base["running_jobs"] = running
        base["abnormal_jobs"] = sum(1 for j in rows if j.get("abnormal"))
        return base