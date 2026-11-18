"""窗口指标结果领域服务：源端批次 → 窗口指标计算 → 结果落地的统一能力。

统一串联 stream_source（源端批次/事件）、window_config（窗口定义）、
sliding_index（指标记录）与 sink_output（结果落地），产出统一的
「窗口指标结果数据」与「落库完整性报告」。

核心一致性保证：
  1. 并发提交只生效一次且保持幂等：同一处理请求（含请求幂等键）并发
     提交时只有第一个请求真正执行，其余请求复用同一批处理结果。
  2. 每个窗口分组在同一来源批次下只能对应一条当前可消费指标记录：
     分组写入以 (batch_id, group_key) 为唯一键，已落稳的分组不会被
     重复计算或覆盖，中断恢复时直接跳过。
  3. 分组事务原子性：一个分组的指标记录与落库回执在同一事务内写入，
     任一步失败整体回滚到事务开始前的同一版本，不会留下半成品。
  4. 发布门禁：结果落地按同一来源批次消费窗口指标结果，未完整落地
     （存在未完成回执）的批次不能进入可发布状态。
  5. 版本化：迟到事件与新窗口定义同时到达时，支持「重算后统一发布」
     与「冻结已发布窗口并生成独立补算版本」两条路径；两条路径都只暴露
     一份完整结果，不同版本都保留在本模块下供页面读取。
"""
from __future__ import annotations

import copy
import threading
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 模块内统一使用的存储表名（store.rows 会按需自动建表）。
BATCH_TABLE = "window_metric_batch"      # 批次处理运行记录
RESULT_TABLE = "window_metric_result"    # 窗口指标结果数据（分组级、版本化）
RECEIPT_TABLE = "sink_output"            # 落库回执（落在结果落地模块，供其详情页核对）
LEDGER_TABLE = "metric_result_ledger"    # 同步台账（另一个业务模块）

# 批次处理状态机。
STATUS_PROCESSING = "处理中"
STATUS_INTERRUPTED = "已中断"
STATUS_COMPLETE = "待发布"
STATUS_PUBLISHED = "已发布"
BATCH_STATUS_ORDER = [
    STATUS_PROCESSING,
    STATUS_INTERRUPTED,
    STATUS_COMPLETE,
    STATUS_PUBLISHED,
]

# 分组/回执状态。
GROUP_DONE = "已落地"
GROUP_PENDING = "待落地"

# 迟到事件 + 新窗口定义的两条补算路径。
MODE_RECOMPUTE = "recompute"   # 重算受影响窗口后统一发布
MODE_FREEZE = "freeze"         # 冻结已发布窗口，生成独立补算版本


def _to_decimal(value: Any) -> Decimal:
    """把任意输入安全地转成 Decimal，无法解析时按 0 处理。"""
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


@dataclass
class _Transaction:
    """分组写入事务：记录受影响表的写入前快照，失败时回滚到同一版本。"""

    touched: dict[str, int]

    def rollback(self) -> None:
        for table, length in self.touched.items():
            del store.rows(table)[length:]


class WindowMetricService(BaseModuleService):
    """窗口指标结果：统一批次处理、版本管理与落库完整性。"""

    MODULE = "window_metric_result"
    ENTITY = "窗口指标结果"
    LIST_FIELDS = [
        "来源批次", "来源名称", "窗口分组", "窗口起", "窗口止",
        "指标版本", "结果数值", "事件条数", "落地状态",
    ]
    REQUIRED_FIELDS = ["来源批次", "窗口分组"]
    STATUS_ORDER = [GROUP_PENDING, GROUP_DONE]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    # 模块内串行化所有写操作，保证并发提交与事务的线程安全。
    _lock = threading.RLock()
    # 幂等键 → 运行记录 ID。类级共享：即使路由层与调用方分别实例化服务，
    # 并发/重复提交仍能命中同一处理结果，只生效一次。
    _idempotency: dict[str, int] = {}

    @classmethod
    def reset_idempotency(cls) -> None:
        """清空幂等记录（仅供测试隔离与运维重置使用）。"""
        with cls._lock:
            cls._idempotency.clear()

    # ------------------------------------------------------------------
    # 源端批次读取
    # ------------------------------------------------------------------

    def list_source_batches(self) -> list[dict[str, Any]]:
        """列出源端工作台已生产、可发起窗口指标处理的来源批次。"""
        batches = [
            row for row in store.rows("stream_source")
            if isinstance(row.get("batch_id"), str) and isinstance(row.get("events"), list)
        ]
        result = []
        for row in sorted(batches, key=lambda b: b.get("produced_at", 0), reverse=True):
            batch_id = row["batch_id"]
            run = self._latest_run(batch_id)
            result.append({
                "batch_id": batch_id,
                "source": row.get("source", ""),
                "topic": row.get("topic", ""),
                "event_count": len(row.get("events", [])),
                "produced_at": row.get("produced_at", 0),
                "processed": run is not None,
                "run_status": run.get("status") if run else None,
                "published_version": run.get("published_version") if run else None,
            })
        return result

    def _get_source_batch(self, batch_id: str) -> dict[str, Any] | None:
        for row in store.rows("stream_source"):
            if row.get("batch_id") == batch_id and isinstance(row.get("events"), list):
                return row
        return None

    # ------------------------------------------------------------------
    # 窗口指标计算（纯函数，便于单测）
    # ------------------------------------------------------------------

    @staticmethod
    def compute_group_metrics(events: list[dict[str, Any]]) -> dict[str, float]:
        """按一组事件计算 count/sum/avg/max/min 指标（amount 缺省为 0）。"""
        values = [_to_decimal(e.get("payload", {}).get("amount", 0.0)) for e in events]
        if not values:
            return {"count": 0.0, "sum": 0.0, "avg": 0.0, "max": 0.0, "min": 0.0}
        total = sum(values, Decimal("0"))
        return {
            "count": float(len(values)),
            "sum": float(total),
            "avg": float(total / len(values)),
            "max": float(max(values)),
            "min": float(min(values)),
        }

    def _window_events(
        self,
        events: list[dict[str, Any]],
        window_size: float,
        slide: float,
    ) -> list[tuple[str, float, float, list[dict[str, Any]]]]:
        """按窗口大小/步长切分事件，返回 (window_key, start, end, events)。

        直接按事件时间计算所属窗口桶（滑动窗口下一个事件可落入多个重叠
        窗口），只产出含事件的窗口；即使迟到事件与批次事件时间跨度极大，
        也不会遍历中间的空窗口区间。
        """
        if not events:
            return []
        ordered = sorted(events, key=lambda e: e.get("event_time", 0.0))
        origin = ordered[0]["event_time"]
        # 一个事件最多同时属于 ceil(window_size / slide) 个重叠窗口。
        span_buckets = max(int(-(-window_size // slide)), 1)
        buckets: dict[int, list[dict[str, Any]]] = {}
        for event in ordered:
            offset = event.get("event_time", origin) - origin
            last_idx = int(offset // slide)
            # 以事件为右端点回溯它落入的全部窗口桶；窗口索引可以为负，
            # 这样才能保留首个事件之前开启的重叠窗口。
            for idx in range(last_idx - span_buckets + 1, last_idx + 1):
                buckets.setdefault(idx, []).append(event)
        windows: list[tuple[str, float, float, list[dict[str, Any]]]] = []
        for idx in sorted(buckets):
            w_start = origin + idx * slide
            w_end = w_start + window_size
            in_window = [
                e for e in buckets[idx]
                if w_start <= e.get("event_time", 0.0) < w_end
            ]
            if in_window:
                windows.append((f"w_{int(w_start)}_{int(w_end)}", w_start, w_end, in_window))
        return windows

    def _build_group_records(
        self,
        batch_id: str,
        source: str,
        events: list[dict[str, Any]],
        window_size: float,
        slide: float,
        group_keys: list[str],
        version: int,
        only_group_keys: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """把事件切成窗口并按分组维度聚合，生成分组级指标记录（尚未落库）。"""
        records: list[dict[str, Any]] = []
        for window_key, w_start, w_end, window_events in self._window_events(
            events, window_size, slide,
        ):
            grouped: dict[str, list[dict[str, Any]]] = {}
            for event in window_events:
                payload = event.get("payload", {})
                group_key = "|".join(str(payload.get(k, "")) for k in group_keys)
                grouped.setdefault(group_key, []).append(event)
            for group_key, group_events in grouped.items():
                unique_key = f"{window_key}#{group_key}"
                if only_group_keys is not None and unique_key not in only_group_keys:
                    continue
                metrics = self.compute_group_metrics(group_events)
                records.append({
                    "来源批次": batch_id,
                    "来源名称": source,
                    "窗口标识": window_key,
                    "窗口分组": group_key,
                    "分组唯一键": unique_key,
                    "窗口起": w_start,
                    "窗口止": w_end,
                    "指标版本": version,
                    "结果数值": metrics,
                    "事件条数": len(group_events),
                    "watermark": w_end - 5.0,
                    "is_current": False,
                    "落地状态": GROUP_PENDING,
                    "status": GROUP_PENDING,
                    "pending": True,
                    "abnormal": False,
                    "computed_at": time.time(),
                })
        return records

    # ------------------------------------------------------------------
    # 分组事务写入：指标记录 + 落库回执 同生共死
    # ------------------------------------------------------------------

    def _write_group_transaction(
        self,
        run: dict[str, Any],
        record: dict[str, Any],
        sink_name: str,
        fail_group_key: str | None,
    ) -> None:
        """在一个事务内写入单个窗口分组的指标记录与落库回执。

        分组写入完成后回执才标记完成；注入 fail_group_key 时模拟该分组
        写入失败，事务整体回滚（用于中断/事务一致性测试）。
        """
        tx = _Transaction({
            RESULT_TABLE: len(store.rows(RESULT_TABLE)),
            RECEIPT_TABLE: len(store.rows(RECEIPT_TABLE)),
            LEDGER_TABLE: len(store.rows(LEDGER_TABLE)),
        })
        try:
            if fail_group_key and record["分组唯一键"] == fail_group_key:
                raise RuntimeError(f"分组 {record['分组唯一键']} 写入失败（模拟中断）")

            now = time.time()
            metric_row = {
                "id": self._next_id(RESULT_TABLE),
                **copy.deepcopy(record),
                "run_id": run["id"],
                "sink": sink_name,
                "is_current": True,
                "落地状态": GROUP_DONE,
                "status": GROUP_DONE,
                "pending": False,
                "landed_at": now,
            }
            store.rows(RESULT_TABLE).append(metric_row)

            # 同一来源批次消费窗口指标结果：分组写入完成后回执才标记完成。
            receipt = {
                "id": self._next_id(RECEIPT_TABLE),
                "配置名称": sink_name,
                "输出类型": "window_metric",
                "目标地址": f"window_metric://{run['batch_id']}",
                "写入模式": "transactional",
                "分区策略": "group_key",
                "格式转换": "json",
                "批量大小": "1",
                "receipt_kind": "window_metric_receipt",
                "batch_id": run["batch_id"],
                "run_id": run["id"],
                "group_key": record["分组唯一键"],
                "window_key": record["窗口标识"],
                "metric_version": record["指标版本"],
                "write_status": "success",
                "status": "已生效",
                "pending": False,
                "abnormal": False,
                "written_at": now,
            }
            store.rows(RECEIPT_TABLE).append(receipt)

            # 另一个业务模块同步生成台账。
            ledger_row = {
                "id": self._next_id(LEDGER_TABLE),
                "台账编号": f"LED-{run['batch_id'][:8]}-{record['窗口标识']}-{record['窗口分组']}",
                "来源批次": run["batch_id"],
                "窗口分组": record["分组唯一键"],
                "指标版本": record["指标版本"],
                "事件条数": record["事件条数"],
                "结果摘要": record["结果数值"],
                "落地回执": "success",
                "status": "已登记",
                "pending": False,
                "abnormal": False,
                "recorded_at": now,
            }
            store.rows(LEDGER_TABLE).append(ledger_row)
        except Exception:
            # 事务回滚到同一版本：本分组三张表的写入全部撤销。
            tx.rollback()
            raise

    def _next_id(self, table: str) -> int:
        return max((int(r.get("id", 0)) for r in store.rows(table)), default=0) + 1

    # ------------------------------------------------------------------
    # 批次处理主流程（并发提交只生效一次 + 幂等 + 中断恢复）
    # ------------------------------------------------------------------

    def submit_batch(
        self,
        batch_id: str,
        *,
        sink_name: str = "结果落地样例1",
        window_size: float = 10.0,
        slide: float = 5.0,
        group_keys: list[str] | None = None,
        request_id: str | None = None,
        fail_group_key: str | None = None,
    ) -> dict[str, Any]:
        """按来源批次发起窗口指标处理并落地（并发提交只生效一次，幂等）。"""
        group_keys = group_keys or ["user_id", "region"]
        # 显式幂等键缺省由业务入参派生，保证同参重试天然幂等。
        idem_key = request_id or self._derive_idempotency_key(
            batch_id, sink_name, window_size, slide, group_keys,
        )
        with self._lock:
            existing_id = self._idempotency.get(idem_key)
            if existing_id is not None:
                existing = self._find_run(existing_id)
                if existing is not None:
                    return {
                        "ok": True,
                        "idempotent": True,
                        "run_id": existing_id,
                        "batch_id": batch_id,
                        "message": "并发/重复提交只生效一次，返回首次处理结果",
                        "report": self.integrity_report(batch_id),
                    }

            source_batch = self._get_source_batch(batch_id)
            if source_batch is None:
                return {"ok": False, "error": f"来源批次「{batch_id}」不存在或尚未生产事件"}
            sink = next(
                (r for r in store.rows("sink_output") if r.get("配置名称") == sink_name),
                None,
            )
            if sink is None:
                return {"ok": False, "error": f"结果落地配置「{sink_name}」不存在"}
            if sink.get("status") == "已停用":
                return {"ok": False, "error": f"结果落地配置「{sink_name}」已停用，无法落地"}

            run = self._create_run(
                batch_id, source_batch.get("source", ""), sink_name,
                window_size, slide, group_keys, version=1, mode="initial",
                idem_key=idem_key,
            )
            self._idempotency[idem_key] = run["id"]
            return self._process_run(run, source_batch["events"], fail_group_key)

    def _process_run(
        self,
        run: dict[str, Any],
        events: list[dict[str, Any]],
        fail_group_key: str | None,
        only_group_keys: set[str] | None = None,
    ) -> dict[str, Any]:
        """执行一次处理运行：逐分组事务写入，已落稳分组跳过不重算。"""
        with self._lock:
            run["status"] = STATUS_PROCESSING
            records = self._build_group_records(
                run["batch_id"], run["source"], events,
                run["window_size"], run["slide"], run["group_keys"],
                version=run["version"], only_group_keys=only_group_keys,
            )
            run["group_total"] = len(records)
            landed_keys = {
                r["分组唯一键"]
                for r in store.rows(RESULT_TABLE)
                if r.get("来源批次") == run["batch_id"]
                and r.get("指标版本") == run["version"]
                and r.get("落地状态") == GROUP_DONE
            }
            written = 0
            failed_group: str | None = None
            for record in records:
                # 中断恢复：已落稳的分组不得被重复计算或覆盖。
                if record["分组唯一键"] in landed_keys:
                    run.setdefault("skipped_groups", []).append(record["分组唯一键"])
                    continue
                try:
                    self._write_group_transaction(
                        run, record, run["sink"], fail_group_key,
                    )
                    written += 1
                except Exception as exc:  # 模拟中断：保留现场，等待恢复
                    failed_group = record["分组唯一键"]
                    run["status"] = STATUS_INTERRUPTED
                    run["error"] = str(exc)
                    run["finished_at"] = time.time()
                    return {
                        "ok": False,
                        "interrupted": True,
                        "run_id": run["id"],
                        "batch_id": run["batch_id"],
                        "written_groups": written,
                        "failed_group": failed_group,
                        "message": f"分组 {failed_group} 写入中断，已回滚该分组，可发起恢复",
                        "report": self.integrity_report(run["batch_id"]),
                    }

            report = self.integrity_report(run["batch_id"])
            # 发布门禁：未完整落地的批次不能进入可发布状态。
            if report["complete"]:
                run["status"] = STATUS_COMPLETE
            else:
                run["status"] = STATUS_INTERRUPTED
            run["finished_at"] = time.time()
            return {
                "ok": report["complete"],
                "idempotent": False,
                "run_id": run["id"],
                "batch_id": run["batch_id"],
                "written_groups": written,
                "skipped_groups": run.get("skipped_groups", []),
                "message": "批次窗口指标已全部计算并完整落地"
                if report["complete"] else "批次仍有分组未落地",
                "report": report,
            }

    def resume_batch(self, batch_id: str, request_id: str | None = None) -> dict[str, Any]:
        """中断恢复：跳过已落稳分组，只补算未落地分组。"""
        idem_key = request_id or f"resume:{batch_id}:{int(time.time())}"
        with self._lock:
            if idem_key in self._idempotency:
                run_id = self._idempotency[idem_key]
                return {
                    "ok": True, "idempotent": True, "run_id": run_id,
                    "batch_id": batch_id,
                    "message": "恢复请求重复提交，只生效一次",
                    "report": self.integrity_report(batch_id),
                }
            run = self._latest_run(batch_id)
            if run is None:
                return {"ok": False, "error": f"来源批次「{batch_id}」尚无处理记录"}
            source_batch = self._get_source_batch(batch_id)
            if source_batch is None:
                return {"ok": False, "error": f"来源批次「{batch_id}」事件已不可读"}
            self._idempotency[idem_key] = run["id"]
            return self._process_run(run, source_batch["events"], fail_group_key=None)

    # ------------------------------------------------------------------
    # 发布门禁与版本化（迟到事件 + 新窗口定义两条路径）
    # ------------------------------------------------------------------

    def publish_batch(self, batch_id: str) -> dict[str, Any]:
        """发布批次：仅完整落地的批次可发布；发布后每个分组只暴露一份当前结果。"""
        with self._lock:
            run = self._latest_run(batch_id)
            if run is None:
                return {"ok": False, "error": f"来源批次「{batch_id}」尚无处理记录"}
            report = self.integrity_report(batch_id)
            if not report["complete"]:
                return {
                    "ok": False,
                    "error": "批次未完整落地，不能进入可发布状态",
                    "report": report,
                }
            version = run["version"]
            self._promulate_current(batch_id, version)
            run["status"] = STATUS_PUBLISHED
            run["published_version"] = version
            run["published_at"] = time.time()
            return {
                "ok": True,
                "batch_id": batch_id,
                "published_version": version,
                "message": f"批次已发布，当前可消费版本 v{version}",
                "report": report,
            }

    def revise_for_late_events(
        self,
        batch_id: str,
        late_events: list[dict[str, Any]],
        *,
        mode: str = MODE_RECOMPUTE,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """迟到事件与新窗口定义同时到达时的两条补算路径。

        recompute：重算受影响窗口后统一发布，全分组提升到新版本。
        freeze：冻结已发布窗口，仅对受影响窗口生成独立补算版本，
                未受影响分组继续消费旧版本，两条路径都只暴露一份完整结果。
        """
        if mode not in (MODE_RECOMPUTE, MODE_FREEZE):
            return {"ok": False, "error": f"不支持的补算模式：{mode}"}
        idem_key = request_id or f"revise:{batch_id}:{mode}:{len(late_events)}"
        with self._lock:
            if idem_key in self._idempotency:
                run_id = self._idempotency[idem_key]
                return {
                    "ok": True, "idempotent": True, "run_id": run_id,
                    "batch_id": batch_id,
                    "message": "补算并发提交只生效一次",
                    "report": self.integrity_report(batch_id),
                }
            source_batch = self._get_source_batch(batch_id)
            if source_batch is None:
                return {"ok": False, "error": f"来源批次「{batch_id}」不存在"}
            base_run = self._latest_run(batch_id)
            if base_run is None:
                return {"ok": False, "error": "批次尚未处理，无法补算"}
            new_version = self._next_version(batch_id)
            all_events = [*source_batch["events"], *late_events]

            affected_keys = {
                record["分组唯一键"]
                for record in self._build_group_records(
                    batch_id, source_batch.get("source", ""), late_events,
                    base_run["window_size"], base_run["slide"],
                    base_run["group_keys"], version=new_version,
                )
            }
            scope = None if mode == MODE_RECOMPUTE else affected_keys
            run = self._create_run(
                batch_id, source_batch.get("source", ""), base_run["sink"],
                base_run["window_size"], base_run["slide"], base_run["group_keys"],
                version=new_version, mode=mode, idem_key=idem_key,
                affected_groups=sorted(affected_keys),
            )
            self._idempotency[idem_key] = run["id"]
            process_result = self._process_run(
                run, all_events, fail_group_key=None, only_group_keys=scope,
            )
            if not process_result["ok"]:
                return process_result

            if mode == MODE_RECOMPUTE:
                # 重算后统一发布：新版本成为全部分组的唯一当前结果。
                self._promulate_current(batch_id, new_version)
            else:
                # 冻结模式：受影响窗口切到补算版本，其余分组保留已发布版本。
                self._promulate_current(batch_id, new_version, only_keys=affected_keys)
            run["status"] = STATUS_PUBLISHED
            run["published_version"] = new_version
            run["published_at"] = time.time()
            return {
                "ok": True,
                "idempotent": False,
                "run_id": run["id"],
                "batch_id": batch_id,
                "mode": mode,
                "version": new_version,
                "affected_groups": sorted(affected_keys),
                "message": "重算统一发布完成" if mode == MODE_RECOMPUTE
                else "已冻结已发布窗口，独立补算版本仅覆盖受影响窗口",
                "report": self.integrity_report(batch_id),
            }

    def _promulate_current(
        self, batch_id: str, version: int, only_keys: set[str] | None = None,
    ) -> None:
        """把指定版本标记为当前可消费版本，每个分组只保留一条当前记录。"""
        for row in store.rows(RESULT_TABLE):
            if row.get("来源批次") != batch_id:
                continue
            if only_keys is not None and row.get("分组唯一键") not in only_keys:
                continue
            row["is_current"] = int(row.get("指标版本", 0)) == version

    # ------------------------------------------------------------------
    # 查询：结果数据、完整性报告、版本
    # ------------------------------------------------------------------

    def list_results(
        self,
        batch_id: str | None = None,
        *,
        current_only: bool = False,
        version: int | None = None,
    ) -> list[dict[str, Any]]:
        """按来源批次查看窗口指标结果数据。"""
        rows = store.rows(RESULT_TABLE)
        if batch_id:
            rows = [r for r in rows if r.get("来源批次") == batch_id]
        if version is not None:
            rows = [r for r in rows if r.get("指标版本") == version]
        if current_only:
            rows = [r for r in rows if r.get("is_current")]
        return sorted(
            rows,
            key=lambda r: (r.get("来源批次", ""), r.get("指标版本", 0), r.get("分组唯一键", "")),
        )

    def list_runs(self) -> list[dict[str, Any]]:
        """列出全部批次处理运行（跨模块联动列表页数据源）。"""
        return sorted(
            store.rows(BATCH_TABLE),
            key=lambda r: r.get("created_at", 0), reverse=True,
        )

    def integrity_report(self, batch_id: str) -> dict[str, Any]:
        """按来源批次生成落库完整性报告与每个分组的写入状态。"""
        results = [
            r for r in store.rows(RESULT_TABLE)
            if r.get("来源批次") == batch_id
        ]
        receipts = [
            r for r in store.rows(RECEIPT_TABLE)
            if r.get("batch_id") == batch_id and r.get("receipt_kind") == "window_metric_receipt"
        ]
        run = self._latest_run(batch_id)
        expected_total = run.get("group_total", len({r.get("分组唯一键") for r in results})) if run else 0
        # 当前可消费视图：每个窗口分组恰好一条。
        current = [r for r in results if r.get("is_current")]
        current_keys = [r.get("分组唯一键") for r in current]
        groups = []
        for row in results:
            groups.append({
                "group_key": row.get("分组唯一键"),
                "window_key": row.get("窗口标识"),
                "version": row.get("指标版本"),
                "event_count": row.get("事件条数"),
                "landed": row.get("落地状态") == GROUP_DONE,
                "is_current": bool(row.get("is_current")),
                "has_receipt": any(
                    rc.get("group_key") == row.get("分组唯一键")
                    and rc.get("metric_version") == row.get("指标版本")
                    for rc in receipts
                ),
            })
        landed = sum(1 for g in groups if g["landed"] and g["has_receipt"])
        versions = sorted({r.get("指标版本", 0) for r in results})
        return {
            "batch_id": batch_id,
            "source": run.get("source") if run else None,
            "sink": run.get("sink") if run else None,
            "run_status": run.get("status") if run else None,
            "expected_groups": expected_total,
            "landed_groups": landed,
            "pending_groups": max(expected_total - landed, 0),
            "receipt_count": len(receipts),
            "complete": expected_total > 0 and landed >= expected_total,
            "publishable": bool(run and run.get("status") == STATUS_COMPLETE),
            "published_version": run.get("published_version") if run else None,
            "versions": versions,
            "current_consumable_groups": len(current),
            "current_unique": len(current_keys) == len(set(current_keys)),
            "groups": sorted(groups, key=lambda g: str(g["group_key"])),
        }

    def overview_cards(self) -> dict[str, Any]:
        """1号概览看板使用的窗口指标结果汇总。"""
        results = store.rows(RESULT_TABLE)
        runs = store.rows(BATCH_TABLE)
        return {
            "batch_runs": len(runs),
            "published_batches": sum(1 for r in runs if r.get("status") == STATUS_PUBLISHED),
            "interrupted_batches": sum(1 for r in runs if r.get("status") == STATUS_INTERRUPTED),
            "result_records": len(results),
            "current_records": sum(1 for r in results if r.get("is_current")),
            "complete_batches": sum(1 for r in runs if r.get("status") in (STATUS_COMPLETE, STATUS_PUBLISHED)),
        }

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------

    def _create_run(
        self,
        batch_id: str,
        source: str,
        sink_name: str,
        window_size: float,
        slide: float,
        group_keys: list[str],
        *,
        version: int,
        mode: str,
        idem_key: str,
        affected_groups: list[str] | None = None,
    ) -> dict[str, Any]:
        run = {
            "id": self._next_id(BATCH_TABLE),
            "batch_id": batch_id,
            "source": source,
            "sink": sink_name,
            "window_size": window_size,
            "slide": slide,
            "group_keys": group_keys,
            "version": version,
            "mode": mode,
            "affected_groups": affected_groups or [],
            "group_total": 0,
            "status": STATUS_PROCESSING,
            "idempotency_key": idem_key,
            "skipped_groups": [],
            "created_at": time.time(),
            "finished_at": None,
            "published_version": None,
        }
        store.rows(BATCH_TABLE).append(run)
        return run

    def _find_run(self, run_id: int) -> dict[str, Any] | None:
        return next(
            (r for r in store.rows(BATCH_TABLE) if r.get("id") == run_id), None,
        )

    def _latest_run(self, batch_id: str) -> dict[str, Any] | None:
        runs = [r for r in store.rows(BATCH_TABLE) if r.get("batch_id") == batch_id]
        return max(runs, key=lambda r: r.get("id", 0)) if runs else None

    def _next_version(self, batch_id: str) -> int:
        versions = [
            r.get("指标版本", 1) for r in store.rows(RESULT_TABLE)
            if r.get("来源批次") == batch_id
        ]
        return (max(versions) if versions else 0) + 1

    @staticmethod
    def _derive_idempotency_key(
        batch_id: str, sink_name: str, window_size: float,
        slide: float, group_keys: list[str],
    ) -> str:
        raw = "|".join([
            batch_id, sink_name, str(window_size), str(slide), ",".join(group_keys),
        ])
        import hashlib
        return hashlib.md5(raw.encode()).hexdigest()[:16]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base.update(self.overview_cards())
        return base
