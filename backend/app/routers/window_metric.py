"""窗口指标结果专用接口：批次处理、中断恢复、发布、补算版本与完整性报告。

路由统一挂在 /api/window-metrics 前缀下，与通用资源路由 /api/{module}
区分开；在 main.py 中先于资源路由注册，避免动态路径抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.window_metric import MODE_FREEZE, MODE_RECOMPUTE, WindowMetricService
from app.services.stream_source import StreamSourceService
from app.services.sink_output import SinkOutputService

router = APIRouter(prefix="/api/window-metrics", tags=["窗口指标结果"])

service = WindowMetricService()
source_service = StreamSourceService()
sink_service = SinkOutputService()


class SubmitPayload(BaseModel):
    batch_id: str
    sink_name: str = "结果落地样例1"
    window_size: float = 10.0
    slide: float = 5.0
    group_keys: list[str] = Field(default_factory=lambda: ["user_id", "region"])
    request_id: str | None = None
    # 测试/演练用：指定分组唯一键注入写入失败，模拟批次处理中断。
    fail_group_key: str | None = None


class ResumePayload(BaseModel):
    batch_id: str
    request_id: str | None = None


class PublishPayload(BaseModel):
    batch_id: str


class RevisePayload(BaseModel):
    batch_id: str
    late_events: list[dict[str, Any]] = Field(default_factory=list)
    mode: str = MODE_RECOMPUTE
    request_id: str | None = None


class ProducePayload(BaseModel):
    source_name: str
    batch_size: int = 20


@router.get("/source-batches")
def get_source_batches() -> dict[str, Any]:
    """列出可发起处理的源端批次及当前处理/发布状态。"""
    return {"items": service.list_source_batches(), "total": len(service.list_source_batches())}


@router.post("/source-batches/produce")
def produce_source_batch(payload: ProducePayload) -> dict[str, Any]:
    """在源端工作台按批次生产事件（沿流式源端调用链入口）。"""
    return source_service.produce_source_snapshot(payload.source_name, payload.batch_size)


@router.post("/submit")
def submit_batch(payload: SubmitPayload) -> dict[str, Any]:
    """按来源批次读取事件、计算窗口指标并写入结果落地（幂等，并发只生效一次）。"""
    return service.submit_batch(
        payload.batch_id,
        sink_name=payload.sink_name,
        window_size=payload.window_size,
        slide=payload.slide,
        group_keys=payload.group_keys,
        request_id=payload.request_id,
        fail_group_key=payload.fail_group_key,
    )


@router.post("/resume")
def resume_batch(payload: ResumePayload) -> dict[str, Any]:
    """中断恢复：已落稳分组跳过，只补算未落地分组。"""
    return service.resume_batch(payload.batch_id, request_id=payload.request_id)


@router.post("/publish")
def publish_batch(payload: PublishPayload) -> dict[str, Any]:
    """发布批次：未完整落地的批次不能进入可发布状态。"""
    return service.publish_batch(payload.batch_id)


@router.post("/revise")
def revise_batch(payload: RevisePayload) -> dict[str, Any]:
    """迟到事件与新窗口定义同时到达：recompute 统一重算 / freeze 独立补算版本。"""
    return service.revise_for_late_events(
        payload.batch_id,
        payload.late_events,
        mode=payload.mode,
        request_id=payload.request_id,
    )


@router.get("/runs")
def list_runs() -> dict[str, Any]:
    """跨模块联动列表页：全部批次处理运行记录。"""
    items = service.list_runs()
    return {"items": items, "total": len(items)}


@router.get("/runs/{batch_id}/report")
def get_report(batch_id: str) -> dict[str, Any]:
    """按来源批次核对窗口指标结果数据和落库完整性报告。"""
    return service.integrity_report(batch_id)


@router.get("/results")
def list_results(
    batch_id: str | None = None,
    current_only: bool = False,
    version: int | None = None,
) -> dict[str, Any]:
    """1号清单视图：按来源批次读取窗口指标结果数据，支持版本/当前视图过滤。"""
    items = service.list_results(batch_id, current_only=current_only, version=version)
    return {"items": items, "total": len(items)}


@router.get("/overview")
def window_metric_overview() -> dict[str, Any]:
    """1号概览看板：窗口指标结果数据汇总卡片。"""
    return service.overview_cards()


@router.get("/sink-detail/{entry_id}")
def sink_detail(entry_id: int) -> dict[str, Any]:
    """1号详情页：结果落地配置下的窗口指标结果与每个分组写入状态。"""
    detail = sink_service.get_window_metric_detail(entry_id)
    if detail is None:
        return {"ok": False, "error": f"输出配置 {entry_id} 不存在"}
    return {"ok": True, **detail}
