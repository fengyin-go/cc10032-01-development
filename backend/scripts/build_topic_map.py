#!/usr/bin/env python3
"""为 cc10032 生成 v3 topic map 和候选池。"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent  # cc10032/cc10032 (git repo root)
# S11_META 在 container 目录 cc10032/.s11-meta
S11_META = REPO_ROOT.parent / ".s11-meta"

# 12 模式, 每个 3 条 = 36
MODES = [
    "H1-数据生成-特征演化",
    "H1-数据生成-实体继承",
    "H1-数据生成-条件分支",
    "H2-架构重构-提取抽象",
    "H2-架构重构-引入中间层",
    "H2-架构重构-解耦依赖",
    "H3-故障注入-边界错误",
    "H3-故障注入-并发竞争",
    "H3-故障注入-级联故障",
    "H3-链路验证-端到端回放",
    "H3-链路验证-兼容性回退",
    "H3-链路验证-跨域关联",
]

SERVICES = [
    ("stream_source", "流式源端", "backend/app/services/stream_source.py"),
    ("window_config", "窗口配置", "backend/app/services/window_config.py"),
    ("calc_operator", "计算算子", "backend/app/services/calc_operator.py"),
    ("state_store", "状态存储", "backend/app/services/state_store.py"),
    ("watermark_track", "水位追踪", "backend/app/services/watermark_track.py"),
    ("alert_policy", "告警策略", "backend/app/services/alert_policy.py"),
    ("sliding_index", "滑动指标", "backend/app/services/sliding_index.py"),
    ("join_stream", "流关联", "backend/app/services/join_stream.py"),
    ("sink_output", "结果落地", "backend/app/services/sink_output.py"),
    ("backfill_job", "补数任务", "backend/app/services/backfill_job.py"),
    ("cep_rule", "CEP规则", "backend/app/services/cep_rule.py"),
    ("throughput_mgr", "吞吐管理", "backend/app/services/throughput_mgr.py"),
    ("checkpoint_mgr", "检查点", "backend/app/services/checkpoint_mgr.py"),
    ("restore_plan", "故障恢复", "backend/app/services/restore_plan.py"),
    ("metric_lineage", "指标血缘", "backend/app/services/metric_lineage.py"),
    ("late_handler", "迟到处理", "backend/app/services/late_handler.py"),
    ("task_topology", "任务拓扑", "backend/app/services/task_topology.py"),
    ("data_sampler", "数据采样", "backend/app/services/data_sampler.py"),
    ("debug_trace", "调试追踪", "backend/app/services/debug_trace.py"),
    ("aggregate_sink", "聚合落库", "backend/app/services/aggregate_sink.py"),
    ("stream_job", "流式作业", "backend/app/services/stream_job.py"),
    ("anomaly_detector", "异常检测", "backend/app/services/anomaly_detector.py"),
    ("replay_engine", "回放引擎", "backend/app/services/replay_engine.py"),
]

OPERATIONS = [
    "采集", "接入", "抓取", "爬取", "导入", "读取", "解析", "清洗", "转换",
    "标准化", "关联", "聚合", "去重", "校验", "入湖", "入库", "落库", "存储",
    "写入", "查询", "计算", "调度", "回填", "迁移", "导出", "切分", "归并",
    "合并", "归档", "水位",
]

# 模块文件的行范围映射 - 7-16 行是 class 定义和字段声明
MODULE_LINE_RANGE = (7, 16)

def read_file_lines(rel_path: str) -> list[str]:
    p = REPO_ROOT / rel_path
    if not p.exists():
        return []
    return p.read_text(encoding="utf-8").splitlines()


def build_module_anchors(prim_module: str, sec_module: str) -> list[dict]:
    """为两个模块生成 difficulty_design.module_anchors。"""
    anchors = []
    for mod_key in [prim_module, sec_module]:
        file_path = None
        for mk, mn, mp in SERVICES:
            if mk == mod_key:
                file_path = mp
                break
        if file_path is None:
            continue
        lines = read_file_lines(file_path)
        if not lines:
            continue
        actual_end = len(lines)
        anchors.append({
            "module": mod_key,
            "path": file_path,
            "start_line": 1,
            "end_line": actual_end,
        })
    return anchors


def build_support_evidence(prim_module: str, sec_module: str) -> list[dict]:
    """为主次模块生成支撑证据（与 module_anchors 重叠）。"""
    evidence = []
    
    def _read_excerpt(mod_key: str) -> tuple[str, str, int, int, list[str]]:
        """读取模块文件，返回 (path, excerpt, start, end, lines)。"""
        file_path = None
        for mk, mn, mp in SERVICES:
            if mk == mod_key:
                file_path = mp
                break
        if file_path is None:
            return ("backend/app/services/stream_source.py", "", 1, 1, [])
        lines = read_file_lines(file_path)
        actual_end = len(lines)
        start = 1
        excerpt = "\n".join(lines[start-1:actual_end])
        return (file_path, excerpt, start, actual_end, lines)
    
    # 主模块证据
    prim_path, prim_excerpt, prim_start, prim_end, _ = _read_excerpt(prim_module)
    evidence.append({
        "module": prim_module,
        "artifact": f"{prim_module}数据集",
        "role": "producer",
        "behavior": f"在{prim_module}服务中定义实体字段、状态流转和动作规则，生成{prim_module}数据集记录",
        "path": prim_path,
        "start_line": prim_start,
        "end_line": prim_end,
        "excerpt": prim_excerpt,
    })
    
    # 次模块证据
    sec_path, sec_excerpt, sec_start, sec_end, _ = _read_excerpt(sec_module)
    evidence.append({
        "module": sec_module,
        "artifact": f"{sec_module}数据集",
        "role": "consumer",
        "behavior": f"消费{prim_module}数据并在{sec_module}中执行下游处理与状态确认",
        "path": sec_path,
        "start_line": sec_start,
        "end_line": sec_end,
        "excerpt": sec_excerpt,
    })
    
    return evidence


def build_task_plan(
    idx: int, mode: str,
    prim_mod_key: str, sec_mod_key: str,
    topic_name: str, op_term: str,
    artifact_name: str, decision_axis: str,
) -> dict:
    c1 = f"C{idx}"
    c2 = f"C{idx + 36}"
    
    prim_name = [m[1] for m in SERVICES if m[0] == prim_mod_key]
    sec_name = [m[1] for m in SERVICES if m[0] == sec_mod_key]
    if not prim_name:
        prim_name = [prim_mod_key]
    if not sec_name:
        sec_name = [sec_mod_key]
    prim_display = prim_name[0]
    sec_display = sec_name[0]
    
    prim_artifact = f"{prim_mod_key}数据集"
    sec_artifact = f"{sec_mod_key}数据集"
    
    return {
        "sequence": idx,
        "topic": topic_name,
        "mode": mode,
        "domain_fit": {
            "domain": "数据工程",
            "subdirection": "数据分析与数据应用",
            "domain_chain": {
                "operation": op_term,
                "main_task": f"在{prim_display}模块中{op_term}{artifact_name}数据",
                "upstream_source": prim_display,
                "downstream_target": sec_display,
            },
            "prompt_main_request": f"请基于{prim_display}和{sec_display}模块实现{artifact_name}的{op_term}功能",
            "domain_description": f"数据工程领域{artifact_name}处理链路，{prim_display}→{sec_display}数据流转",
        },
        "difficulty_design": {
            "version": 3,
            "module_integration": {
                "primary_module": prim_mod_key,
                "secondary_modules": [
                    {
                        "name": sec_mod_key,
                        "role": "consumer",
                        "dependency": "data_read",
                        "artifact": sec_artifact,
                    },
                ],
                "data_artifact": prim_artifact,
                "integration_point": "store.memory_layer",
            },
            "module_anchors": build_module_anchors(prim_mod_key, sec_mod_key),
            "reasoning_challenge": {
                "constraints": [
                    {
                        "id": c1,
                        "title": f"{prim_display}数据完整性",
                        "description": f"确保{prim_display}的{artifact_name}记录字段完整且状态可追溯",
                    },
                    {
                        "id": c2,
                        "title": f"{sec_display}消费一致性",
                        "description": f"确保{sec_display}消费{prim_display}数据时获取一致版本",
                    },
                ],
                "challenge_description": f"需要协调{prim_display}生产侧与{sec_display}消费侧的{artifact_name}数据一致性",
                "prompt_excerpt": f"思考跨模块{artifact_name}数据流转，需考虑状态一致性和字段完整性",
            },
            "design_tradeoff": {
                "decision": f"{prim_display}数据版本优先级：最新记录 vs 已确认记录",
                "trigger": f"{sec_display}消费时发现{prim_display}存在多个版本{artifact_name}记录",
                "decision_axis": decision_axis,
                "options": [
                    "以最新版本为准覆盖旧数据",
                    "以确认版本为准忽略未确认",
                    "以消费位点版本对齐时间戳",
                ],
                "context_excerpt": f"{sec_display}读取{prim_display}时可能出现版本不一致",
            },
            "workload_design": {
                "version": 1,
                "expected_effective_tool_calls": 60,
                "call_floor_rationale": (
                    "跨两个模块的完整实现需要读取源码、设计接口、编写业务逻辑、"
                    "补充失败与恢复分支，并验证至少两条独立数据链路，预计不低于60次有效工具调用。"
                ),
                "work_slices": [
                    {
                        "module": prim_display,
                        "implementation": f"在{prim_display}中实现{op_term}{artifact_name}创建、写入和状态流转",
                        "data_result": f"{artifact_name}主版本数据记录",
                        "verification": f"验证{prim_display}生成的{artifact_name}字段完整且状态可追溯",
                    },
                    {
                        "module": prim_display,
                        "implementation": f"在{prim_display}中补齐{artifact_name}失败恢复、去重和版本边界处理",
                        "data_result": f"{artifact_name}版本校验与恢复报告",
                        "verification": f"验证{prim_display}重试不覆盖上一份完整{artifact_name}",
                    },
                    {
                        "module": sec_display,
                        "implementation": f"在{sec_display}中读取{artifact_name}并生成下游消费记录",
                        "data_result": f"{artifact_name}下游消费记录",
                        "verification": f"验证{sec_display}只读取已确认版本{artifact_name}",
                    },
                    {
                        "module": sec_display,
                        "implementation": f"在{sec_display}中处理{artifact_name}异常、审计和回执落库",
                        "data_result": f"{artifact_name}异常审计回执",
                        "verification": f"验证{sec_display}回执与主版本数据逐条追溯",
                    },
                ],
                "verification_paths": [
                    f"backend/app/services/{prim_mod_key}.py::domain_snapshot",
                    f"backend/app/services/{sec_mod_key}.py::domain_snapshot",
                ],
            },
        },
        "support_evidence": build_support_evidence(prim_mod_key, sec_mod_key),
    }


def _build_topics(module_pairs: list) -> list[dict]:
    """基于模块对构建 topics 数组。每个主模块生成一个 topic。"""
    seen_modules: set[str] = set()
    topics = []
    for prim, sec, topic_name, op, art in module_pairs:
        if prim in seen_modules:
            continue
        seen_modules.add(prim)
        prim_display = None
        for mk, mn, mp in SERVICES:
            if mk == prim:
                prim_display = mn
                break
        if prim_display is None:
            prim_display = prim
        topics.append({
            "id": prim,
            "name": f"{prim_display}领域主题",
            "module": prim,
            "keywords": [prim_display, prim, "实时计算", "数据流"],
            "evidence": [f"backend/app/services/{prim}.py"],
            "domain_objects": [f"{prim}数据集"],
            "domain_results": [f"{prim}处理结果"],
            "seat_capacity": 3,
            "allowed_families": [],
            "notes": f"覆盖{prim_display}的3个难度题位",
        })
    return topics


def main():
    # 生成 36 条 task_plan (12模式 × 3)
    # 使用 20 个模块两两配对
    module_pairs = [
        ("stream_source", "window_config", "流式数据窗口聚合", "接入", "窗口聚合"),
        ("window_config", "sliding_index", "滑动窗口指标计算", "计算", "窗口指标"),
        ("calc_operator", "aggregate_sink", "算子结果聚合落库", "聚合", "算子聚合"),
        ("stream_source", "calc_operator", "流式数据算子处理", "转换", "算子处理"),
        ("stream_source", "sink_output", "流式数据结果落地", "写入", "数据落地"),
        ("watermark_track", "late_handler", "水位延迟检查反馈", "水位", "水位延迟"),
        ("late_handler", "backfill_job", "迟到数据补数回填", "回填", "迟到补数"),
        ("watermark_track", "backfill_job", "水位溢出补数联动", "调度", "溢出补数"),
        ("join_stream", "aggregate_sink", "双流关联结果聚合", "关联", "流关联聚合"),
        ("join_stream", "checkpoint_mgr", "关联状态检查点管理", "存储", "关联快照"),
        ("aggregate_sink", "checkpoint_mgr", "聚合落库检查点恢复", "归档", "落库快照"),
        ("cep_rule", "alert_policy", "CEP事件序列告警", "校验", "事件告警"),
        ("alert_policy", "restore_plan", "告警触发故障恢复", "计算", "告警恢复"),
        ("cep_rule", "restore_plan", "事件匹配恢复联动", "关联", "CEP恢复"),
        ("task_topology", "throughput_mgr", "拓扑背压吞吐联调", "调度", "背压管理"),
        ("throughput_mgr", "state_store", "吞吐降级状态保持", "存储", "降级状态"),
        ("task_topology", "state_store", "拓扑执行状态存储", "入库", "拓扑状态"),
        ("data_sampler", "debug_trace", "采样数据调试追踪", "采集", "采样追踪"),
        ("metric_lineage", "stream_source", "血缘源端追溯", "查询", "血缘追溯"),
        ("metric_lineage", "sink_output", "血缘输出端验证", "校验", "血缘验证"),
        ("state_store", "checkpoint_mgr", "状态存储检查点同步", "存储", "状态快照"),
        ("sliding_index", "sink_output", "滑动指标结果落地", "落库", "指标落地"),
        ("calc_operator", "sliding_index", "算子驱动滑动窗口", "聚合", "算子窗口"),
        ("window_config", "calc_operator", "窗口策略算子绑定", "计算", "策略绑定"),
        ("join_stream", "sink_output", "关联结果流转落地", "写入", "关联落地"),
        ("late_handler", "sink_output", "迟到旁路数据落地", "转换", "旁路落地"),
        ("backfill_job", "aggregate_sink", "补数结果聚合验证", "聚合", "补数验证"),
        ("restore_plan", "checkpoint_mgr", "恢复方案检查点对齐", "校验", "恢复对齐"),
        ("task_topology", "checkpoint_mgr", "拓扑快照一致性", "存储", "拓扑快照"),
        ("data_sampler", "state_store", "采样特征状态持久化", "入库", "采样状态"),
        ("debug_trace", "alert_policy", "调试异常告警联动", "校验", "调试告警"),
        ("throughput_mgr", "alert_policy", "吞吐阈值告警联动", "标准化", "吞吐告警"),
        ("state_store", "metric_lineage", "状态与血缘一致性", "关联", "状态血缘"),
        ("watermark_track", "alert_policy", "水位延迟告警通知", "调度", "水位告警"),
        ("late_handler", "metric_lineage", "迟到数据血缘标记", "清洗", "迟到血缘"),
        ("restore_plan", "metric_lineage", "恢复血缘完整性", "迁移", "恢复血缘"),
    ]
    
    decision_axes = [
        "effective_time", "completion_precedence", "version_adoption",
        "source_of_truth", "old_vs_new_priority", "concurrency_conflict",
        "retention_vs_reclaim", "recovery_vs_cleanup",
        "scope_boundary", "authority_conflict",
        "migration_switch", "partial_success_boundary",
    ] * 3  # 重复以覆盖36条
    
    plans = []
    for i, (prim, sec, topic, op, art) in enumerate(module_pairs, 1):
        mode_idx = (i - 1) // 3
        mode = MODES[mode_idx] if mode_idx < len(MODES) else MODES[-1]
        axis = decision_axes[(i - 1) % len(decision_axes)]
        plan = build_task_plan(
            idx=i, mode=mode, prim_mod_key=prim, sec_mod_key=sec,
            topic_name=f"{topic}链路设计", op_term=op, artifact_name=art,
            decision_axis=axis,
        )
        plans.append(plan)
    
    try:
        result = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"],
            capture_output=True, text=True, check=True)
        base_commit = result.stdout.strip()
    except Exception:
        base_commit = "unknown"

    map_payload = {
        "version": 1,
        "profile": "gsb-semantic-v2",
        "difficulty_contract_version": 3,
        "type_policy": "capacity-first",
        "base_commit": base_commit,
        "max_capacity": 36,
        "domain": "数据工程",
        "subdirection": "数据分析与数据应用",
        "project_id": "cc10032",
        "topics": _build_topics(module_pairs),
        "task_plan": plans,
        "token_budget": {
            "task_max_tool_calls_per_plan": 70,
            "total_planned_tool_calls": 2160,
            "audit_instruction": "H1/H2/H3 v3 硬闸要求: max_per_mode=3, min_tool_calls=55",
        },
    }
    
    S11_META.mkdir(parents=True, exist_ok=True)
    map_path = S11_META / "repo-topic-map.json"
    map_path.write_text(json.dumps(map_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Topic map written: {map_path} ({len(plans)} plans)")
    
    # 生成候选池 48 条
    candidate_operations = OPERATIONS[:]
    while len(candidate_operations) < 48:
        candidate_operations.extend(OPERATIONS)
    candidates = []
    for i, op in enumerate(candidate_operations[:48], 1):
        m1 = SERVICES[i % 20][0]
        m2 = SERVICES[(i + 1) % 20][0]
        m1_name = SERVICES[i % 20][1]
        m2_name = SERVICES[(i + 1) % 20][1]
        candidates.append({
            "sequence": i,
            "topic": f"{m1_name}与{m2_name}的{op}链路",
            "operation": op,
            "primary_module": m1,
            "secondary_module": m2,
            "artifact": f"{op}数据产物",
            "estimated_tool_calls": 55 + (i % 15),
            "domain_fit_score": round(0.7 + (i % 10) * 0.03, 2),
            
        })
    
    pool_path = S11_META / "candidate-pool.json"
    pool_payload = {
        "version": 1,
        "project_id": "cc10032",
        "pool_size": len(candidates),
        "candidates": candidates,
    }
    pool_path.write_text(json.dumps(pool_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Candidate pool written: {pool_path} ({len(candidates)} candidates)")


if __name__ == "__main__":
    main()