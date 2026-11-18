import { fetchJson, request } from './client'

export type SourceBatch = {
  batch_id: string
  source: string
  topic: string
  event_count: number
  produced_at: number
  processed: boolean
  run_status: string | null
  published_version: number | null
}

export type IntegrityGroup = {
  group_key: string
  window_key: string
  version: number
  event_count: number
  landed: boolean
  is_current: boolean
  has_receipt: boolean
}

export type IntegrityReport = {
  batch_id: string
  source: string | null
  sink: string | null
  run_status: string | null
  expected_groups: number
  landed_groups: number
  pending_groups: number
  receipt_count: number
  complete: boolean
  publishable: boolean
  published_version: number | null
  versions: number[]
  current_consumable_groups: number
  current_unique: boolean
  groups: IntegrityGroup[]
}

export type WindowMetricRow = {
  id: number
  来源批次: string
  来源名称: string
  窗口标识: string
  窗口分组: string
  分组唯一键: string
  窗口起: number
  窗口止: number
  指标版本: number
  结果数值: Record<string, number>
  事件条数: number
  is_current: boolean
  落地状态: string
  sink: string
}

export type BatchRun = {
  id: number
  batch_id: string
  source: string
  sink: string
  version: number
  mode: string
  group_total: number
  status: string
  created_at: number
  finished_at: number | null
  published_version: number | null
  skipped_groups: string[]
}

type ActionEnvelope = { ok: boolean; message?: string; [key: string]: unknown }

async function post(path: string, body: Record<string, unknown>): Promise<ActionEnvelope> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  return (await response.json()) as ActionEnvelope
}

export function listSourceBatches(): Promise<{ items: SourceBatch[]; total: number }> {
  return fetchJson('/api/window-metrics/source-batches')
}

export function produceSourceBatch(sourceName: string, batchSize = 20) {
  return post('/api/window-metrics/source-batches/produce', {
    source_name: sourceName,
    batch_size: batchSize,
  })
}

export function submitWindowBatch(
  batchId: string,
  options: { sink_name?: string; request_id?: string; fail_group_key?: string | null } = {},
) {
  return post('/api/window-metrics/submit', {
    batch_id: batchId,
    sink_name: options.sink_name ?? '结果落地样例1',
    request_id: options.request_id ?? null,
    fail_group_key: options.fail_group_key ?? null,
  })
}

export function resumeWindowBatch(batchId: string) {
  return post('/api/window-metrics/resume', { batch_id: batchId })
}

export function publishWindowBatch(batchId: string) {
  return post('/api/window-metrics/publish', { batch_id: batchId })
}

export function reviseWindowBatch(
  batchId: string,
  mode: 'recompute' | 'freeze',
  lateEvents: Record<string, unknown>[],
) {
  return post('/api/window-metrics/revise', {
    batch_id: batchId,
    mode,
    late_events: lateEvents,
  })
}

export function listWindowResults(
  filters: { batch_id?: string; current_only?: boolean; version?: number } = {},
): Promise<{ items: WindowMetricRow[]; total: number }> {
  const query = new URLSearchParams()
  if (filters.batch_id) query.set('batch_id', filters.batch_id)
  if (filters.current_only) query.set('current_only', 'true')
  if (filters.version !== undefined) query.set('version', String(filters.version))
  return fetchJson(`/api/window-metrics/results?${query.toString()}`)
}

export function listBatchRuns(): Promise<{ items: BatchRun[]; total: number }> {
  return fetchJson('/api/window-metrics/runs')
}

export function getIntegrityReport(batchId: string): Promise<IntegrityReport> {
  return fetchJson(`/api/window-metrics/runs/${batchId}/report`)
}

export function getWindowMetricOverview(): Promise<Record<string, number>> {
  return fetchJson('/api/window-metrics/overview')
}

export function getSinkMetricDetail(entryId: number) {
  return fetchJson<{ ok: boolean } & Record<string, unknown>>(
    `/api/window-metrics/sink-detail/${entryId}`,
  )
}
