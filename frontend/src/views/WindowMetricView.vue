<template>
  <section class="page" data-module="window_metric_result">
    <header class="page-head">
      <div>
        <h2>窗口指标结果</h2>
        <p class="page-desc">
          从源端批次读取事件、计算窗口指标并写入结果落地；按来源批次核对窗口指标结果数据与落库完整性报告。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh" type="button" @click="reloadAll">刷新</button>
        <RouterLink class="btn" to="/sink_output">前往结果落地核对</RouterLink>
      </div>
    </header>

    <!-- 1号概览看板：窗口指标结果数据 -->
    <div class="stat-row" data-testid="overview-cards">
      <article v-for="card in overviewCards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>

    <p v-if="errorMessage" class="error-text" data-testid="error-text">{{ errorMessage }}</p>

    <!-- 源端工作台：按批次发起处理 -->
    <h3 class="section-title">源端工作台 · 按来源批次处理</h3>
    <table class="data-table" data-testid="source-batch-table">
      <thead>
        <tr>
          <th>来源批次</th><th>来源名称</th><th>事件数</th><th>处理状态</th>
          <th>已发布版本</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="batch in sourceBatches" :key="batch.batch_id" :data-testid="`batch-${batch.batch_id}`">
          <td>{{ batch.batch_id }}</td>
          <td>{{ batch.source }}</td>
          <td>{{ batch.event_count }}</td>
          <td>
            <span :data-testid="`status-${batch.batch_id}`">{{ batch.run_status ?? '未处理' }}</span>
          </td>
          <td>{{ batch.published_version ? `v${batch.published_version}` : '—' }}</td>
          <td class="row-actions">
            <button class="link" data-testid="submit-batch" type="button" @click="onSubmit(batch.batch_id)">
              发起处理
            </button>
            <button
              class="link"
              type="button"
              :disabled="batch.run_status !== '已中断'"
              @click="onResume(batch.batch_id)"
            >
              中断恢复
            </button>
            <button class="link" type="button" @click="onPublish(batch.batch_id)">发布</button>
            <button class="link" type="button" @click="openRevise(batch.batch_id)">迟到补算</button>
            <button class="link" type="button" @click="selectBatch(batch.batch_id)">查看结果</button>
          </td>
        </tr>
        <tr v-if="!sourceBatches.length">
          <td colspan="6" class="empty-state">暂无源端批次，可在流式源端工作台生产事件批次</td>
        </tr>
      </tbody>
    </table>

    <!-- 跨模块联动列表页：批次处理运行记录 -->
    <h3 class="section-title">批次处理运行 · 跨模块联动列表</h3>
    <table class="data-table" data-testid="run-table">
      <thead>
        <tr>
          <th>运行ID</th><th>来源批次</th><th>来源</th><th>落地配置</th><th>版本</th>
          <th>补算模式</th><th>分组总数</th><th>运行状态</th><th>跳过分组</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="run in runs" :key="run.id" :data-testid="`run-${run.id}`" :data-status="run.status">
          <td>{{ run.id }}</td>
          <td>{{ run.batch_id }}</td>
          <td>{{ run.source }}</td>
          <td>{{ run.sink }}</td>
          <td>v{{ run.version }}</td>
          <td>{{ modeLabel(run.mode) }}</td>
          <td>{{ run.group_total }}</td>
          <td>{{ run.status }}</td>
          <td>{{ run.skipped_groups?.length ?? 0 }}</td>
        </tr>
        <tr v-if="!runs.length">
          <td colspan="9" class="empty-state">尚无批次处理运行</td>
        </tr>
      </tbody>
    </table>

    <!-- 列表视图：按来源批次核对窗口指标结果数据 -->
    <div class="filter-bar">
      <label class="filter-item">
        <span>来源批次过滤</span>
        <select v-model="filterBatch" data-testid="filter-batch" @change="reloadResults">
          <option value="">全部批次</option>
          <option v-for="batch in sourceBatches" :key="batch.batch_id" :value="batch.batch_id">
            {{ batch.batch_id }}
          </option>
        </select>
      </label>
      <label class="filter-item">
        <span>版本</span>
        <select v-model="filterVersion" data-testid="filter-version" @change="reloadResults">
          <option :value="undefined">全部版本</option>
          <option v-for="version in reportVersions" :key="version" :value="version">v{{ version }}</option>
        </select>
      </label>
      <label class="filter-item">
        <span>仅当前可消费</span>
        <input v-model="currentOnly" data-testid="filter-current" type="checkbox" @change="reloadResults" />
      </label>
    </div>

    <!-- 落库完整性报告 -->
    <div v-if="report" class="report-card" data-testid="integrity-report">
      <h4>落库完整性报告 · {{ report.batch_id }}</h4>
      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">分组总数</span><strong>{{ report.expected_groups }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">已落地</span><strong>{{ report.landed_groups }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">待落地</span><strong>{{ report.pending_groups }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">回执数</span><strong>{{ report.receipt_count }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">完整落地</span>
          <strong data-testid="report-complete">{{ report.complete ? '是' : '否' }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">可发布</span><strong>{{ report.publishable ? '是' : '否' }}</strong>
        </article>
      </div>
      <p class="page-desc">
        历史版本：{{ report.versions.map((v) => `v${v}`).join('、') || '—' }} ｜
        当前可消费分组：{{ report.current_consumable_groups }} ｜
        每分组唯一当前记录：{{ report.current_unique ? '是' : '否' }}
      </p>
      <table class="data-table">
        <thead>
          <tr><th>窗口分组</th><th>窗口</th><th>版本</th><th>事件数</th><th>已落地</th><th>回执</th><th>当前可消费</th></tr>
        </thead>
        <tbody>
          <tr v-for="group in report.groups" :key="`${group.group_key}-${group.version}`">
            <td>{{ group.group_key }}</td>
            <td>{{ group.window_key }}</td>
            <td>v{{ group.version }}</td>
            <td>{{ group.event_count }}</td>
            <td>{{ group.landed ? '✓' : '✗' }}</td>
            <td>{{ group.has_receipt ? '✓' : '✗' }}</td>
            <td>{{ group.is_current ? '✓' : '—' }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- 1号清单视图：窗口指标结果数据 -->
    <h3 class="section-title">窗口指标结果清单</h3>
    <table class="data-table" data-testid="result-table">
      <thead>
        <tr>
          <th>来源批次</th><th>窗口分组</th><th>窗口起</th><th>窗口止</th>
          <th>指标版本</th><th>结果数值</th><th>事件条数</th><th>落地状态</th><th>当前</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in results" :key="row.id" :data-testid="`result-${row.id}`">
          <td>{{ row.来源批次 }}</td>
          <td>{{ row.窗口分组 }}</td>
          <td>{{ formatTs(row.窗口起) }}</td>
          <td>{{ formatTs(row.窗口止) }}</td>
          <td>v{{ row.指标版本 }}</td>
          <td><code>{{ formatMetrics(row.结果数值) }}</code></td>
          <td>{{ row.事件条数 }}</td>
          <td>{{ row.落地状态 }}</td>
          <td>{{ row.is_current ? '✓' : '—' }}</td>
        </tr>
        <tr v-if="!results.length">
          <td colspan="9" class="empty-state">暂无窗口指标结果数据</td>
        </tr>
      </tbody>
    </table>

    <!-- 迟到事件补算弹窗 -->
    <div v-if="reviseOpen" class="modal-backdrop" data-testid="revise-modal">
      <form class="modal" @submit.prevent="submitRevise">
        <h3>迟到事件补算 · {{ reviseBatchId }}</h3>
        <label class="create-field">
          <span>处理路径</span>
          <select v-model="reviseMode" data-testid="revise-mode">
            <option value="recompute">重算受影响窗口后统一发布</option>
            <option value="freeze">冻结已发布窗口，生成独立补算版本</option>
          </select>
        </label>
        <label class="create-field">
          <span>迟到事件数（模拟注入）</span>
          <input v-model.number="lateCount" data-testid="late-count" type="number" min="1" max="20" />
        </label>
        <p class="page-desc">两条路径都只暴露一份完整结果，历史版本保留在本模块下供页面读取。</p>
        <div class="page-actions">
          <button class="btn" type="button" @click="reviseOpen = false">取消</button>
          <button class="btn primary" data-testid="submit-revise" type="submit">确认补算</button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ results.length }} 条窗口指标结果，刷新后接口实时读取</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  getIntegrityReport,
  listBatchRuns,
  listSourceBatches,
  listWindowResults,
  publishWindowBatch,
  resumeWindowBatch,
  reviseWindowBatch,
  submitWindowBatch,
} from '@/api/window-metric'
import type {
  BatchRun,
  IntegrityReport,
  SourceBatch,
  WindowMetricRow,
} from '@/api/window-metric'
import { fetchJson } from '@/api/client'

const sourceBatches = ref<SourceBatch[]>([])
const runs = ref<BatchRun[]>([])
const results = ref<WindowMetricRow[]>([])
const report = ref<IntegrityReport | null>(null)
const overview = ref<Record<string, number>>({})
const errorMessage = ref('')

const filterBatch = ref('')
const filterVersion = ref<number | undefined>(undefined)
const currentOnly = ref(false)

const reviseOpen = ref(false)
const reviseBatchId = ref('')
const reviseMode = ref<'recompute' | 'freeze'>('recompute')
const lateCount = ref(3)

const overviewCards = computed(() => [
  { label: '批次处理运行', value: overview.value.batch_runs ?? 0 },
  { label: '已发布批次', value: overview.value.published_batches ?? 0 },
  { label: '中断批次', value: overview.value.interrupted_batches ?? 0 },
  { label: '结果记录数', value: overview.value.result_records ?? 0 },
  { label: '当前可消费记录', value: overview.value.current_records ?? 0 },
  { label: '完整落地批次', value: overview.value.complete_batches ?? 0 },
])

const reportVersions = computed(() => report.value?.versions ?? [])

function modeLabel(mode: string): string {
  if (mode === 'initial') return '首次处理'
  if (mode === 'recompute') return '重算统一发布'
  if (mode === 'freeze') return '冻结补算版本'
  return mode
}

function formatTs(ts: number): string {
  return new Date(ts * 1000).toLocaleString('zh-CN', { hour12: false })
}

function formatMetrics(metrics: Record<string, number>): string {
  return Object.entries(metrics)
    .map(([key, value]) => `${key}=${Number(value).toFixed(2)}`)
    .join('  ')
}

async function reloadAll() {
  errorMessage.value = ''
  try {
    const [batches, runPage, cards] = await Promise.all([
      listSourceBatches(),
      listBatchRuns(),
      fetchJson<Record<string, number>>('/api/window-metrics/overview'),
    ])
    sourceBatches.value = batches.items
    runs.value = runPage.items
    overview.value = cards
    await reloadResults()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '窗口指标数据读取失败'
  }
}

async function reloadResults() {
  try {
    const page = await listWindowResults({
      batch_id: filterBatch.value || undefined,
      current_only: currentOnly.value,
      version: filterVersion.value,
    })
    results.value = page.items
    if (filterBatch.value) {
      report.value = await getIntegrityReport(filterBatch.value)
    } else if (sourceBatches.value.length) {
      report.value = await getIntegrityReport(sourceBatches.value[0].batch_id)
    } else {
      report.value = null
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '结果数据读取失败'
  }
}

function selectBatch(batchId: string) {
  filterBatch.value = batchId
  filterVersion.value = undefined
  void reloadResults()
}

async function guard(action: () => Promise<{ ok: boolean; message?: string }>) {
  try {
    const result = await action()
    if (!result.ok) throw new Error(result.message ?? '操作失败')
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

function onSubmit(batchId: string) {
  void guard(() => submitWindowBatch(batchId))
}

function onResume(batchId: string) {
  void guard(() => resumeWindowBatch(batchId))
}

function onPublish(batchId: string) {
  void guard(() => publishWindowBatch(batchId))
}

function openRevise(batchId: string) {
  reviseBatchId.value = batchId
  reviseOpen.value = true
}

async function submitRevise() {
  const lateEvents = Array.from({ length: lateCount.value }, (_, index) => ({
    event_id: `late-${Date.now()}-${index}`,
    payload: {
      user_id: `u${index % 5}`,
      amount: 50 + index * 7,
      region: `region_${index % 4}`,
    },
    // 迟到事件：事件时间远早于当前批次。
    event_time: 1.0 + index,
    source: 'late_handler',
  }))
  reviseOpen.value = false
  await guard(() => reviseWindowBatch(reviseBatchId.value, reviseMode.value, lateEvents))
}

onMounted(reloadAll)
</script>
