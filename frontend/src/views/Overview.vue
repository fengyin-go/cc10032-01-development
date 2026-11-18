<template>
  <section class="page">
    <header class="page-head">
      <div>
        <h2>运营概览</h2>
        <p class="page-desc">汇总各业务模块的关键指标，先看总量再看异常。</p>
      </div>
    </header>
    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>

    <h3 class="section-title">窗口指标结果数据</h3>
    <div class="stat-row" data-testid="window-metric-cards">
      <article v-for="card in windowMetricCards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>
    <RouterLink class="btn" to="/window_metric_result">进入窗口指标结果工作台</RouterLink>
    <table class="data-table">
      <thead>
        <tr><th>业务模块</th><th>今日新增</th><th>待处理</th><th>异常量</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in moduleRows" :key="row.name">
          <td>{{ row.name }}</td>
          <td>{{ row.created }}</td>
          <td>{{ row.pending }}</td>
          <td>{{ row.abnormal }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { fetchJson } from '@/api/client'

type Overview = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
  window_metric?: Record<string, number>
}

const cards = ref<Overview['cards']>([])
const moduleRows = ref<Overview['modules']>([])
const windowMetric = ref<Record<string, number>>({})

const windowMetricCards = computed(() => [
  { label: '批次处理运行', value: windowMetric.value.batch_runs ?? 0 },
  { label: '已发布批次', value: windowMetric.value.published_batches ?? 0 },
  { label: '中断批次', value: windowMetric.value.interrupted_batches ?? 0 },
  { label: '结果记录数', value: windowMetric.value.result_records ?? 0 },
  { label: '当前可消费记录', value: windowMetric.value.current_records ?? 0 },
  { label: '完整落地批次', value: windowMetric.value.complete_batches ?? 0 },
])

onMounted(async () => {
  try {
    const payload = await fetchJson<Overview>('/api/overview')
    cards.value = payload.cards
    moduleRows.value = payload.modules
    windowMetric.value = payload.window_metric ?? {}
  } catch {
    cards.value = [{"label": "业务模块", "value": 0}, {"label": "今日新增", "value": 0}]
    moduleRows.value = []
  }
})
</script>
