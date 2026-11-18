<template>
  <section class="page" data-testid="sink-detail">
    <header class="page-head">
      <div>
        <h2>结果落地详情 · 窗口指标结果</h2>
        <p class="page-desc">按来源批次核对每个窗口分组的写入状态、落库回执与最终可消费版本。</p>
      </div>
      <div class="page-actions">
        <RouterLink class="btn" to="/sink_output">返回结果落地</RouterLink>
        <RouterLink class="btn primary" to="/window_metric_result">窗口指标结果工作台</RouterLink>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <div v-if="detail" class="report-card">
      <h3>{{ detail.sink?.['配置名称'] }}（{{ detail.sink?.['输出类型'] }}）</h3>
      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">关联来源批次</span><strong>{{ detail.batch_count }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">落库回执总数</span><strong>{{ detail.receipt_total }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">当前可消费结果</span><strong>{{ detail.current_results.length }}</strong>
        </article>
      </div>
    </div>

    <h3 class="section-title">每个分组的写入状态与回执</h3>
    <table class="data-table" data-testid="group-write-table">
      <thead>
        <tr><th>来源批次</th><th>窗口</th><th>窗口分组</th><th>版本</th><th>写入状态</th><th>写入时间</th></tr>
      </thead>
      <tbody>
        <tr v-for="(item, index) in detail?.group_write_status ?? []" :key="`${item.batch_id}-${item.group_key}-${index}`">
          <td>{{ item.batch_id }}</td>
          <td>{{ item.window_key }}</td>
          <td>{{ item.group_key }}</td>
          <td>v{{ item.version }}</td>
          <td>{{ item.write_status }}</td>
          <td>{{ formatTs(item.written_at) }}</td>
        </tr>
        <tr v-if="!detail?.group_write_status?.length">
          <td colspan="6" class="empty-state">该输出配置下尚无窗口指标落库回执</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">按来源批次的落库完整性报告</h3>
    <table class="data-table" data-testid="sink-report-table">
      <thead>
        <tr>
          <th>来源批次</th><th>运行状态</th><th>分组总数</th><th>已落地</th>
          <th>待落地</th><th>完整落地</th><th>已发布版本</th><th>历史版本</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="report in detail?.integrity_reports ?? []" :key="report.batch_id">
          <td>{{ report.batch_id }}</td>
          <td>{{ report.run_status }}</td>
          <td>{{ report.expected_groups }}</td>
          <td>{{ report.landed_groups }}</td>
          <td>{{ report.pending_groups }}</td>
          <td>{{ report.complete ? '是' : '否' }}</td>
          <td>{{ report.published_version ? `v${report.published_version}` : '—' }}</td>
          <td>{{ report.versions.map((v: number) => `v${v}`).join('、') }}</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">当前可消费窗口指标结果数据</h3>
    <table class="data-table" data-testid="sink-result-table">
      <thead>
        <tr><th>来源批次</th><th>窗口分组</th><th>版本</th><th>事件条数</th><th>结果数值</th><th>落地状态</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in detail?.current_results ?? []" :key="row.id">
          <td>{{ row['来源批次'] }}</td>
          <td>{{ row['窗口分组'] }}</td>
          <td>v{{ row['指标版本'] }}</td>
          <td>{{ row['事件条数'] }}</td>
          <td><code>{{ formatMetrics(row['结果数值']) }}</code></td>
          <td>{{ row['落地状态'] }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'

import { getSinkMetricDetail } from '@/api/window-metric'

type SinkDetail = {
  ok: boolean
  sink: Record<string, string>
  receipt_total: number
  batch_count: number
  batch_ids: string[]
  group_write_status: {
    batch_id: string
    group_key: string
    window_key: string
    version: number
    write_status: string
    written_at: number
  }[]
  integrity_reports: {
    batch_id: string
    run_status: string
    expected_groups: number
    landed_groups: number
    pending_groups: number
    complete: boolean
    published_version: number | null
    versions: number[]
  }[]
  current_results: {
    id: number
    来源批次: string
    窗口分组: string
    指标版本: number
    事件条数: number
    结果数值: Record<string, number>
    落地状态: string
  }[]
}

const route = useRoute()
const detail = ref<SinkDetail | null>(null)
const errorMessage = ref('')

function formatTs(ts: number): string {
  return ts ? new Date(ts * 1000).toLocaleString('zh-CN', { hour12: false }) : '—'
}

function formatMetrics(metrics: Record<string, number>): string {
  return Object.entries(metrics)
    .map(([key, value]) => `${key}=${Number(value).toFixed(2)}`)
    .join('  ')
}

onMounted(async () => {
  try {
    const entryId = Number(route.params.entryId)
    detail.value = (await getSinkMetricDetail(entryId)) as unknown as SinkDetail
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '结果落地详情读取失败'
  }
})
</script>
