import type {
  AnalyticsDailyRow,
  AnalyticsKpi,
  ApiChannel,
  ApiJob,
  ApiVideo,
  AppSettingsResponse,
  DashboardSummary,
  DashboardTask,
  YoutubeApiUsage,
} from '../types'

const API_BASE = '/api'

type ApiListResponse<T> = {
  data: T[]
  total: number
  page: number
  limit: number
}

async function request<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${url}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  })

  const payload = await response.json().catch(() => ({}))
  if (!response.ok) {
    const error = new Error(String(payload?.error || response.statusText || 'Request failed')) as Error & {
      status?: number
      data?: unknown
    }
    error.status = response.status
    error.data = payload
    throw error
  }

  return payload as T
}

function withQuery(path: string, params?: Record<string, string | number | null | undefined>): string {
  const query = new URLSearchParams()
  Object.entries(params || {}).forEach(([key, value]) => {
    if (value == null || value === '') return
    query.set(key, String(value))
  })
  const suffix = query.toString()
  return suffix ? `${path}?${suffix}` : path
}

export const api = {
  getSettings: () => request<AppSettingsResponse>('/settings'),
  updateSettings: (payload: Partial<AppSettingsResponse>) =>
    request<AppSettingsResponse>('/settings', { method: 'PATCH', body: JSON.stringify(payload) }),
  getYoutubeApiUsage: () => request<YoutubeApiUsage>('/settings/youtube-api-usage'),
  runDailySync: () => request<{ job_id: string; status: string; message?: string }>('/sync/daily', { method: 'POST' }),

  getChannels: (params?: Record<string, string | number | null | undefined>) =>
    request<ApiListResponse<ApiChannel>>(withQuery('/channels', params)),
  getChannel: (channelId: string) => request<ApiChannel>(`/channels/${encodeURIComponent(channelId)}`),
  addChannel: (payload: Record<string, unknown>) =>
    request<ApiChannel & { sync_job_id?: string }>('/channels', { method: 'POST', body: JSON.stringify(payload) }),
  updateChannel: (channelId: string, payload: Record<string, unknown>) =>
    request<ApiChannel>(`/channels/${encodeURIComponent(channelId)}`, { method: 'PATCH', body: JSON.stringify(payload) }),
  deleteChannel: (channelId: string) =>
    request<{ success?: boolean }>(`/channels/${encodeURIComponent(channelId)}`, { method: 'DELETE' }),
  syncChannel: (channelId: string) =>
    request<{ job_id: string; status: string }>(`/channels/${encodeURIComponent(channelId)}/sync`, { method: 'POST' }),

  getVideos: (params?: Record<string, string | number | null | undefined>) =>
    request<ApiListResponse<ApiVideo>>(withQuery('/videos', params)),
  getDashboard: (params?: Record<string, string | number | null | undefined>) =>
    request<DashboardSummary>(withQuery('/dashboard', params)),
  getDashboardTasks: (params?: Record<string, string | number | null | undefined>) =>
    request<ApiListResponse<DashboardTask>>(withQuery('/dashboard/tasks', params)),
  createDashboardTask: (payload: Record<string, unknown>) =>
    request<DashboardTask>('/dashboard/tasks', { method: 'POST', body: JSON.stringify(payload) }),
  updateDashboardTask: (taskId: string, payload: Record<string, unknown>) =>
    request<DashboardTask>(`/dashboard/tasks/${encodeURIComponent(taskId)}`, { method: 'PATCH', body: JSON.stringify(payload) }),
  deleteDashboardTask: (taskId: string) =>
    request<{ success?: boolean }>(`/dashboard/tasks/${encodeURIComponent(taskId)}`, { method: 'DELETE' }),
  getKpi: (channelId: string, range = '28d') =>
    request<AnalyticsKpi>(withQuery(`/channels/${encodeURIComponent(channelId)}/analytics/kpi`, { range })),
  getDailyTable: (channelId: string, params?: Record<string, string | number | null | undefined>) =>
    request<ApiListResponse<AnalyticsDailyRow>>(withQuery(`/channels/${encodeURIComponent(channelId)}/analytics/daily-table`, params)),

  getJobs: (params?: Record<string, string | number | null | undefined>) =>
    request<ApiListResponse<ApiJob>>(withQuery('/jobs', params)),
  createJob: (type: string, payload: Record<string, unknown>) =>
    request<{ job_id: string; type: string; status: string }>('/jobs', {
      method: 'POST',
      body: JSON.stringify({ type, payload }),
    }),
}
