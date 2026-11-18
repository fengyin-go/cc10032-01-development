"""窗口指标结果领域测试：批次处理、幂等并发、事务回滚、中断恢复、发布门禁与版本化。

覆盖需求要点：
  - 从源端批次读取事件、计算窗口指标并写入结果落地的完整链路
  - 并发提交只生效一次且保持幂等
  - 每个窗口分组只对应一条当前可消费指标记录，回执在分组写入完成后才完成
  - 事务失败整体回滚到同一版本，中断恢复不重复计算/覆盖已落稳分组
  - 未完整落地的批次不能发布；recompute/freeze 两条补算路径都只暴露一份完整结果
"""
from __future__ import annotations

import threading

import pytest

from app.services import service_for
from app.services.stream_source import StreamSourceService
from app.services.window_metric import (
    BATCH_TABLE,
    GROUP_DONE,
    LEDGER_TABLE,
    MODE_FREEZE,
    MODE_RECOMPUTE,
    RECEIPT_TABLE,
    RESULT_TABLE,
    STATUS_COMPLETE,
    STATUS_INTERRUPTED,
    STATUS_PUBLISHED,
    WindowMetricService,
)
from app.store import store


@pytest.fixture()
def ctx():
    """每个用例使用全新来源批次，避免全局内存仓库相互干扰。"""
    WindowMetricService.reset_idempotency()
    source = StreamSourceService()
    service = WindowMetricService()
    produced = source.produce_source_snapshot("流式源端样例2", batch_size=24)
    assert produced["ok"]
    return source, service, produced["batch_id"]


def _probe_records(service: WindowMetricService, batch_id: str):
    batch = service._get_source_batch(batch_id)
    return service._build_group_records(
        batch_id, batch.get("source", ""), batch["events"],
        10.0, 5.0, ["user_id", "region"], version=1,
    )


# ---------------------------------------------------------------------------
# 窗口与指标计算
# ---------------------------------------------------------------------------

class TestWindowComputation:
    def test_group_metrics_aggregation(self):
        service = WindowMetricService()
        events = [
            {"payload": {"amount": v}} for v in (1.0, 2.0, 3.0, 4.0)
        ]
        metrics = service.compute_group_metrics(events)
        assert metrics["count"] == 4
        assert metrics["sum"] == pytest.approx(10.0)
        assert metrics["avg"] == pytest.approx(2.5)
        assert metrics["max"] == 4.0
        assert metrics["min"] == 1.0

    def test_empty_events(self):
        service = WindowMetricService()
        assert service._window_events([], 10.0, 5.0) == []
        assert service.compute_group_metrics([])["count"] == 0

    def test_tumbling_windows_no_overlap(self):
        service = WindowMetricService()
        base = 1000.0
        events = [{"event_time": base + i, "payload": {}} for i in range(20)]
        windows = service._window_events(events, 5.0, 5.0)
        keys = [w[0] for w in windows]
        assert len(keys) == len(set(keys))
        assert len(windows) == 4

    def test_sliding_windows_overlap(self):
        service = WindowMetricService()
        base = 1000.0
        events = [{"event_time": base + i, "payload": {}} for i in range(20)]
        windows = service._window_events(events, 10.0, 5.0)
        # 滑动窗口存在重叠：窗口数多于同尺寸滚动窗口。
        assert len(windows) > 4

    def test_late_event_huge_time_gap_does_not_walk_empty_windows(self):
        """迟到事件与批次事件时间跨度极大时，只产出含事件的窗口。"""
        service = WindowMetricService()
        events = [
            {"event_time": 1.0, "payload": {"user_id": "u0"}},
            {"event_time": 1_700_000_000.0, "payload": {"user_id": "u1"}},
        ]
        windows = service._window_events(events, 10.0, 5.0)
        assert all(w[3] for w in windows)
        assert len(windows) <= 4


# ---------------------------------------------------------------------------
# 批次处理：结果 + 回执 + 台账
# ---------------------------------------------------------------------------

class TestSubmitBatch:
    def test_happy_path_writes_results_receipts_ledger(self, ctx):
        _, service, batch_id = ctx
        expected = len(_probe_records(service, batch_id))
        result = service.submit_batch(batch_id, request_id="submit-1")
        assert result["ok"] is True
        assert result["report"]["complete"] is True

        results = service.list_results(batch_id)
        assert len(results) == expected
        assert all(r["落地状态"] == GROUP_DONE for r in results)

        receipts = [
            r for r in store.rows(RECEIPT_TABLE)
            if r.get("batch_id") == batch_id
        ]
        assert len(receipts) == expected
        assert all(r["write_status"] == "success" for r in receipts)

        ledger = [r for r in store.rows(LEDGER_TABLE) if r.get("来源批次") == batch_id]
        assert len(ledger) == expected

        run = service._latest_run(batch_id)
        assert run["status"] == STATUS_COMPLETE
        assert run["group_total"] == expected

    def test_unknown_batch_rejected(self, ctx):
        _, service, _ = ctx
        result = service.submit_batch("not-exist-batch")
        assert result["ok"] is False
        assert "不存在" in result["error"]

    def test_concurrent_submit_takes_effect_once(self, ctx):
        _, service, batch_id = ctx
        expected = len(_probe_records(service, batch_id))
        responses: list[dict] = []
        barrier = threading.Barrier(8)

        def worker():
            barrier.wait()
            responses.append(service.submit_batch(batch_id, request_id="concurrent-1"))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        run_ids = {r["run_id"] for r in responses}
        assert len(run_ids) == 1
        assert sum(1 for r in responses if r.get("idempotent")) == 7
        # 分组结果没有被并发重复写入。
        assert len(service.list_results(batch_id)) == expected
        runs = [r for r in store.rows(BATCH_TABLE) if r["batch_id"] == batch_id]
        assert len(runs) == 1

    def test_derived_idempotency_key_makes_retry_idempotent(self, ctx):
        _, service, batch_id = ctx
        first = service.submit_batch(batch_id)
        second = service.submit_batch(batch_id)
        assert second["idempotent"] is True
        assert first["run_id"] == second["run_id"]


# ---------------------------------------------------------------------------
# 事务回滚与中断恢复
# ---------------------------------------------------------------------------

class TestTransactionAndResume:
    def test_failed_group_rolls_back_as_a_unit(self, ctx):
        _, service, batch_id = ctx
        probe = _probe_records(service, batch_id)
        fail_key = probe[0]["分组唯一键"]
        result = service.submit_batch(batch_id, request_id="fail-1", fail_group_key=fail_key)

        assert result["interrupted"] is True
        run = service._latest_run(batch_id)
        assert run["status"] == STATUS_INTERRUPTED

        # 失败分组的指标记录、回执、台账在同一事务内全部回滚，不留半成品。
        assert not [
            r for r in store.rows(RESULT_TABLE)
            if r.get("来源批次") == batch_id and r.get("分组唯一键") == fail_key
        ]
        assert not [
            r for r in store.rows(RECEIPT_TABLE)
            if r.get("batch_id") == batch_id and r.get("group_key") == fail_key
        ]
        assert not [
            r for r in store.rows(LEDGER_TABLE)
            if r.get("来源批次") == batch_id and fail_key in str(r.get("窗口分组"))
        ]

    def test_resume_skips_landed_groups_and_completes(self, ctx):
        _, service, batch_id = ctx
        probe = _probe_records(service, batch_id)
        fail_key = probe[3]["分组唯一键"]
        interrupted = service.submit_batch(
            batch_id, request_id="fail-2", fail_group_key=fail_key,
        )
        landed_before = interrupted["report"]["landed_groups"]
        assert landed_before >= 3

        resume = service.resume_batch(batch_id, request_id="resume-1")
        assert resume["ok"] is True
        assert len(resume.get("skipped_groups", [])) == landed_before
        report = service.integrity_report(batch_id)
        assert report["complete"] is True

        # 恢复后每个分组恰好一条指标记录，没有重复计算。
        results = service.list_results(batch_id)
        keys = [r["分组唯一键"] for r in results]
        assert len(keys) == len(set(keys)) == report["expected_groups"]

    def test_resume_is_idempotent(self, ctx):
        _, service, batch_id = ctx
        probe = _probe_records(service, batch_id)
        service.submit_batch(
            batch_id, request_id="fail-3",
            fail_group_key=probe[1]["分组唯一键"],
        )
        service.resume_batch(batch_id, request_id="resume-2")
        again = service.resume_batch(batch_id, request_id="resume-2")
        assert again["idempotent"] is True
        assert service.integrity_report(batch_id)["complete"] is True


# ---------------------------------------------------------------------------
# 发布门禁
# ---------------------------------------------------------------------------

class TestPublishGate:
    def test_incomplete_batch_cannot_publish(self, ctx):
        _, service, batch_id = ctx
        probe = _probe_records(service, batch_id)
        service.submit_batch(
            batch_id, request_id="fail-4",
            fail_group_key=probe[0]["分组唯一键"],
        )
        blocked = service.publish_batch(batch_id)
        assert blocked["ok"] is False
        assert "不能进入可发布状态" in blocked["error"]
        assert service._latest_run(batch_id)["status"] == STATUS_INTERRUPTED

    def test_publish_marks_single_current_per_group(self, ctx):
        _, service, batch_id = ctx
        service.submit_batch(batch_id, request_id="pub-1")
        published = service.publish_batch(batch_id)
        assert published["ok"] is True
        assert published["published_version"] == 1
        assert service._latest_run(batch_id)["status"] == STATUS_PUBLISHED

        current = service.list_results(batch_id, current_only=True)
        keys = [r["分组唯一键"] for r in current]
        assert len(keys) == len(set(keys))
        assert len(current) == service.integrity_report(batch_id)["expected_groups"]


# ---------------------------------------------------------------------------
# 迟到事件 + 新窗口定义：两条补算路径
# ---------------------------------------------------------------------------

class TestRevisionPaths:
    @pytest.fixture()
    def published_batch(self, ctx):
        _, service, batch_id = ctx
        service.submit_batch(batch_id, request_id="base-1")
        service.publish_batch(batch_id)
        late = [{
            "event_id": "LATE-1",
            "payload": {"user_id": "u0", "amount": 99.0, "region": "region_0"},
            "event_time": 1.0,
            "source": "late",
        }]
        return service, batch_id, late

    def test_recompute_unifies_all_groups_on_new_version(self, published_batch):
        service, batch_id, late = published_batch
        result = service.revise_for_late_events(
            batch_id, late, mode=MODE_RECOMPUTE, request_id="rec-1",
        )
        assert result["ok"] is True
        new_version = result["version"]
        assert new_version == 2

        current = service.list_results(batch_id, current_only=True)
        assert current and all(r["指标版本"] == new_version for r in current)
        # 旧版本结果仍然保留，供页面读取历史版本，但不再是当前可消费记录。
        v1 = service.list_results(batch_id, version=1)
        assert v1 and all(not r["is_current"] for r in v1)
        report = service.integrity_report(batch_id)
        assert report["versions"] == [1, 2]
        assert report["complete"] is True

    def test_freeze_keeps_published_windows_and_adds_patch_version(self, published_batch):
        service, batch_id, late = published_batch
        result = service.revise_for_late_events(
            batch_id, late, mode=MODE_FREEZE, request_id="frz-1",
        )
        assert result["ok"] is True
        affected = set(result["affected_groups"])
        assert affected

        current = service.list_results(batch_id, current_only=True)
        # 无论哪种路径，每个窗口分组只暴露一份当前完整结果。
        keys = [r["分组唯一键"] for r in current]
        assert len(keys) == len(set(keys))
        # 受影响窗口切到补算版本，未受影响分组冻结在已发布的 v1。
        for row in current:
            expected_version = 2 if row["分组唯一键"] in affected else 1
            assert row["指标版本"] == expected_version
        assert service._latest_run(batch_id)["status"] == STATUS_PUBLISHED

    def test_revise_concurrent_submit_idempotent(self, published_batch):
        service, batch_id, late = published_batch
        barrier = threading.Barrier(5)
        responses: list[dict] = []

        def worker():
            barrier.wait()
            responses.append(service.revise_for_late_events(
                batch_id, late, mode=MODE_FREEZE, request_id="frz-conc",
            ))

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len({r["run_id"] for r in responses}) == 1
        assert sum(1 for r in responses if r.get("idempotent")) == 4
        keys = [r["分组唯一键"] for r in service.list_results(batch_id, current_only=True)]
        assert len(keys) == len(set(keys))


# ---------------------------------------------------------------------------
# 完整性报告、模块注册与路由
# ---------------------------------------------------------------------------

class TestReportAndRouting:
    def test_integrity_report_shape(self, ctx):
        _, service, batch_id = ctx
        service.submit_batch(batch_id, request_id="rep-1")
        report = service.integrity_report(batch_id)
        for field in (
            "expected_groups", "landed_groups", "pending_groups", "receipt_count",
            "complete", "publishable", "versions", "groups", "current_unique",
        ):
            assert field in report
        assert report["pending_groups"] == 0
        assert all(g["landed"] and g["has_receipt"] for g in report["groups"])

    def test_services_registered_for_resource_router(self):
        assert service_for("window_metric_result") is not None
        assert service_for("metric_result_ledger") is not None

    def test_router_endpoints_and_sink_detail(self, ctx):
        from app.routers import window_metric as router_module
        _, service, batch_id = ctx
        service.submit_batch(batch_id, request_id="route-1")

        batches = router_module.get_source_batches()
        assert any(item["batch_id"] == batch_id for item in batches["items"])

        results = router_module.list_results(batch_id=batch_id)
        assert results["total"] > 0
        report = router_module.get_report(batch_id)
        assert report["complete"] is True
        assert "batch_runs" in router_module.window_metric_overview()

        detail = router_module.sink_detail(1)
        assert detail["ok"] is True
        assert batch_id in detail["batch_ids"]
        assert detail["group_write_status"]

    def test_routes_registered_before_resource_router(self):
        from app.main import app

        def collect(routes, acc):
            for route in routes:
                if hasattr(route, "path"):
                    acc.add(route.path)
                original = getattr(route, "original_router", None)
                if original is not None:
                    collect(original.routes, acc)

        paths: set[str] = set()
        collect(app.routes, paths)
        for path in (
            "/api/window-metrics/submit",
            "/api/window-metrics/resume",
            "/api/window-metrics/publish",
            "/api/window-metrics/revise",
            "/api/window-metrics/results",
            "/api/window-metrics/runs/{batch_id}/report",
        ):
            assert path in paths

    def test_ledger_synced_in_same_version(self, ctx):
        _, service, batch_id = ctx
        service.submit_batch(batch_id, request_id="led-1")
        ledger = service_for("metric_result_ledger").list_by_batch(batch_id)
        results = service.list_results(batch_id)
        assert len(ledger) == len(results)
        assert {r["指标版本"] for r in ledger} == {1}
