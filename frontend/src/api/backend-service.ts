import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export type JobDetail = {
  task_id: string
  status: string
  entity_model: string
  clusters_output: number
  member_count: number
  rule_id: string
  rule_version: string
  original_rule_version: string
  task_time: string
  original_task_time: string
  clusters: {
    簇编号: string
    成员标识: string[]
    规则版本: string
    任务发生时间: string
  }[]
  review_entry: { 簇数量: number; 簇编号: string[]; path: string }
  review_cluster_count: number
  member_recoverable: boolean
  failure_reason: string
}

export async function getJobDetail(id: number): Promise<JobDetail> {
  const response = await request(`/api/auto_match_job/${id}/detail`)
  if (!response.ok) throw new Error('任务详情读取失败')
  return await response.json()
}

export async function retryJob(id: number): Promise<ActionResult & { duplicate?: boolean }> {
  const response = await request(`/api/auto_match_job/${id}/retry`, { method: 'POST' })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}
