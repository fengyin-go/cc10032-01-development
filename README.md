# 实时计算服务中心

面向实时数据分析场景，提供流式计算定义、窗口聚合、策略告警和状态管理的实时数据平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/routers/window_metric.py  窗口指标结果专用接口（批次处理/恢复/发布/补算）
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/window_metric.py  源端批次→窗口指标→结果落地统一能力
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/views/WindowMetricView.vue   窗口指标结果工作台（批次处理+完整性报告）
│   ├── src/views/SinkDetailView.vue     结果落地1号详情页（分组写入状态）
│   ├── src/data/modules.ts              业务模块配置
│   ├── src/api/              统一请求封装
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流式源端 | `stream_source` | 数据源配置 | 源端名称、消息队列、Topic名称 |
| 窗口配置 | `window_config` | 窗口策略 | 策略名称、窗口类型、窗口大小 |
| 计算算子 | `calc_operator` | 算子定义 | 算子名称、算子类型、输入字段 |
| 状态存储 | `state_store` | 状态配置 | 配置名称、后端类型、状态键模式 |
| 水位追踪 | `watermark_track` | 水位记录 | 水位名称、数据源、当前水位 |
| 告警策略 | `alert_policy` | 告警规则 | 规则名称、监控指标、触发条件 |
| 滑动指标 | `sliding_index` | 指标计算 | 指标名称、计算窗口、聚合函数 |
| 流关联 | `join_stream` | 关联配置 | 配置名称、左流源、右流源 |
| 结果落地 | `sink_output` | 输出配置 | 配置名称、输出类型、目标地址 |
| 补数任务 | `backfill_job` | 补数作业 | 作业名称、目标指标、补数范围 |
| CEP规则 | `cep_rule` | 事件序列 | 序列名称、事件类型、匹配顺序 |
| 吞吐管理 | `throughput_mgr` | 吞吐配置 | 配置名称、数据源、最大QPS |
| 检查点 | `checkpoint_mgr` | 检查点配置 | 配置名称、间隔时间、超时时长 |
| 故障恢复 | `restore_plan` | 恢复方案 | 方案名称、任务范围、恢复模式 |
| 指标血缘 | `metric_lineage` | 血缘记录 | 记录ID、指标名称、上游源端 |
| 迟到处理 | `late_handler` | 处理策略 | 策略名称、允许延迟、迟到动作 |
| 任务拓扑 | `task_topology` | 拓扑配置 | 拓扑名称、算子链、并行度 |
| 数据采样 | `data_sampler` | 采样策略 | 策略名称、采样方式、采样比例 |
| 调试追踪 | `debug_trace` | 调试会话 | 会话名称、目标算子、采样数据 |
| 聚合落库 | `aggregate_sink` | 聚合任务 | 任务名称、源指标集、聚合粒度 |
| 窗口指标结果 | `window_metric_result` | 窗口指标结果 | 来源批次、窗口分组、指标版本、落地状态 |
| 指标结果台账 | `metric_result_ledger` | 窗口指标台账 | 台账编号、来源批次、窗口分组、指标版本 |

## 窗口指标结果能力

把源端批次、窗口定义与指标记录统一成一份可消费、可发布、可核对的窗口指标结果数据：

- **统一处理链路**：`POST /api/window-metrics/source-batches/produce` 生产源端事件批次，
  `POST /api/window-metrics/submit` 读取事件、按窗口/分组计算指标并在同一事务内写入结果落地。
- **并发幂等**：同一处理请求（显式 `request_id` 或由入参派生的幂等键）并发/重复提交只生效一次，
  其余请求直接返回首次处理结果。
- **分组事务与中断恢复**：每个窗口分组的指标记录、落库回执、台账在同一事务写入，失败整体回滚；
  `POST /api/window-metrics/resume` 恢复时跳过已落稳分组，不重复计算或覆盖。
- **发布门禁**：`POST /api/window-metrics/publish` 仅对完整落地的批次放行，未落地完成的批次不能发布。
- **两条补算路径**：`POST /api/window-metrics/revise` 支持 `recompute`（重算后统一发布）与
  `freeze`（冻结已发布窗口、生成独立补算版本），两条路径都只暴露一份当前完整结果，历史版本保留。
- **核对视图**：`GET /api/window-metrics/results` 清单、`GET /api/window-metrics/runs/{batch_id}/report`
  落库完整性报告、`GET /api/window-metrics/overview` 概览看板、
  `GET /api/window-metrics/sink-detail/{entry_id}` 结果落地详情页数据。

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据分析与数据应用
