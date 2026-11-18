"""结果落地领域服务：管理流式计算结果的输出落地配置。

数据流角色: state_store / calc_operator → sink_output（消费者+生产者）
增强能力: 批量写入调度、分区策略计算、写入模式切换、异常重试、输出完整性校验
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SinkOutputService(BaseModuleService):
    MODULE = "sink_output"
    ENTITY = "输出配置"
    LIST_FIELDS = ["配置名称", "输出类型", "目标地址", "写入模式", "分区策略", "格式转换", "批量大小"]
    REQUIRED_FIELDS = ["配置名称", "输出类型", "目标地址"]
    STATUS_ORDER = ["草稿", "已生效", "已停用", "写入异常"]
    ACTION_RULES = {"启用输出": "草稿", "停用输出": "已生效"}
    NEGATIVE_ACTIONS = ["停用输出"]

    # ---- 跨模块数据消费与生产 ----

    def batch_write_results(
        self, sink_name: str, results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """将计算结果批量写入目标存储，支持分区和格式转换。"""
        sink = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == sink_name:
                sink = r
                break
        if sink is None:
            return {"ok": False, "error": f"输出配置「{sink_name}」不存在"}
        if sink.get("status") == "已停用":
            return {"ok": False, "error": f"输出配置「{sink_name}」已停用"}

        try:
            batch_size = int(sink.get("批量大小", "100"))
        except (ValueError, TypeError):
            batch_size = 100

        partition_strategy = sink.get("分区策略", "round_robin")
        write_mode = sink.get("写入模式", "append")
        output_type = sink.get("输出类型", "database")
        format_type = sink.get("格式转换", "json")

        written = 0
        failed = 0
        for i, item in enumerate(results):
            # 分区键计算
            partition_key = self._calc_partition(
                item, partition_strategy, i,
            )
            # 格式转换
            converted = self._convert_format(item, format_type)
            record = {
                "id": max(
                    (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
                ) + 1,
                "sink": sink_name,
                "output_type": output_type,
                "partition": partition_key,
                "format": format_type,
                "mode": write_mode,
                "data": converted,
                "written_at": time.time(),
                "status": "已生效",
            }
            # 模拟写入成功/失败
            if self._simulate_write(partition_key):
                record["write_status"] = "success"
                store.rows(self.MODULE).append(record)
                written += 1
            else:
                record["write_status"] = "failed"
                record["异常标记"] = "写入异常"
                store.rows(self.MODULE).append(record)
                failed += 1

        if failed > 0:
            sink["status"] = "写入异常"
            sink["pending"] = True

        return {
            "ok": True,
            "sink": sink_name,
            "total": len(results),
            "written": written,
            "failed": failed,
            "partition_strategy": partition_strategy,
        }

    @staticmethod
    def _calc_partition(
        item: dict[str, Any], strategy: str, index: int,
    ) -> str:
        """计算数据分区键。"""
        if strategy == "hash_key":
            raw = str(item.get("key", index))
            return f"pt_{hashlib.md5(raw.encode()).hexdigest()[:4]}"
        elif strategy == "time_range":
            ts = item.get("timestamp", time.time())
            return time.strftime("dt=%Y%m%d%H", time.localtime(ts))
        elif strategy == "source_based":
            return f"src_{item.get('source', 'default')}"
        else:  # round_robin
            return f"pt_{index % 8:02d}"

    @staticmethod
    def _convert_format(
        item: dict[str, Any], fmt: str,
    ) -> dict[str, Any]:
        """格式转换：JSON/CSV/Parquet 模拟。"""
        if fmt == "csv":
            return {"format": "csv", "raw": str(item)}
        elif fmt == "parquet":
            return {"format": "parquet", "columns": list(item.keys()), "rows": 1}
        return {"format": "json", "payload": item}

    @staticmethod
    def _simulate_write(partition: str) -> bool:
        """模拟写入成功（绝大部分成功，偶发失败模拟异常）。"""
        return hash(partition) % 20 != 0

    def verify_output_integrity(self, sink_name: str) -> dict[str, Any]:
        """验证指定 sink 下的输出记录完整性和写入成功率。"""
        records = [
            r for r in store.rows(self.MODULE)
            if r.get("sink") == sink_name
        ]
        total = len(records)
        success = sum(1 for r in records if r.get("write_status") == "success")
        failed = total - success
        partitions = set(r.get("partition", "") for r in records)
        return {
            "ok": True,
            "sink": sink_name,
            "total": total,
            "success": success,
            "failed": failed,
            "success_rate": success / max(total, 1),
            "partitions": len(partitions),
            "needs_retry": failed > 0,
        }

    def retry_failed_writes(self, sink_name: str) -> dict[str, Any]:
        """重试失败的写入记录。"""
        records = [
            r for r in store.rows(self.MODULE)
            if r.get("sink") == sink_name and r.get("write_status") == "failed"
        ]
        retried = 0
        for record in records:
            if self._simulate_write(record.get("partition", "")):
                record["write_status"] = "success"
                retried += 1
        return {
            "ok": True,
            "sink": sink_name,
            "total_failed": len(records),
            "retried_success": retried,
        }

    def list_sink_destinations(self) -> list[dict[str, Any]]:
        """列出所有活跃的输出目标和状态。"""
        configs = [
            r for r in store.rows(self.MODULE)
            if r.get("id", 0) < 100
        ]
        result = []
        for c in configs:
            name = c.get("配置名称", "")
            records_count = sum(
                1 for r in store.rows(self.MODULE)
                if r.get("sink") == name
            )
            result.append({
                "sink": name,
                "type": c.get("输出类型", ""),
                "target": c.get("目标地址", ""),
                "status": c.get("status", ""),
                "records": records_count,
            })
        return result

    def get_window_metric_detail(self, entry_id: int) -> dict[str, Any] | None:
        """1号详情页：在结果落地详情中呈现窗口指标结果数据与分组写入状态。

        汇总该输出配置名下的窗口指标落库回执、按来源批次的完整性报告，
        以及当前可消费的窗口指标结果数据。
        """
        sink = store.find(self.MODULE, entry_id)
        if sink is None:
            return None
        sink_name = sink.get("配置名称", "")
        receipts = [
            r for r in store.rows(self.MODULE)
            if r.get("receipt_kind") == "window_metric_receipt"
            and r.get("配置名称") == sink_name
        ]
        batch_ids = sorted({r.get("batch_id") for r in receipts if r.get("batch_id")})
        # 延迟导入避免与窗口指标服务产生模块加载循环。
        from app.services.window_metric import WindowMetricService
        metric_service = WindowMetricService()
        reports = [metric_service.integrity_report(bid) for bid in batch_ids]
        current_results = [
            row for row in metric_service.list_results(current_only=True)
            if row.get("sink") == sink_name
        ]
        return {
            "sink": sink,
            "receipt_total": len(receipts),
            "batch_count": len(batch_ids),
            "batch_ids": batch_ids,
            "integrity_reports": reports,
            "current_results": current_results,
            "group_write_status": [
                {
                    "batch_id": r.get("batch_id"),
                    "group_key": r.get("group_key"),
                    "window_key": r.get("window_key"),
                    "version": r.get("metric_version"),
                    "write_status": r.get("write_status"),
                    "written_at": r.get("written_at"),
                }
                for r in receipts
            ],
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["total_records"] = sum(
            1 for r in rows if r.get("write_status")
        )
        base["failed_records"] = sum(
            1 for r in rows if r.get("write_status") == "failed"
        )
        return base