import { useEffect, useMemo, useRef, useState } from 'react'
import './HomePage.css'
import { api } from '../lib/api'
import { formatDateTime, relTime } from '../lib/channelHelpers'
import type { DashboardChannelOverview, DashboardSummary, DashboardTask } from '../types'

const priorityOptions: Array<{ value: DashboardTask['priority']; label: string }> = [
  { value: 'high', label: '高' },
  { value: 'medium', label: '中' },
  { value: 'low', label: '低' },
]
const statusOptions: Array<{ value: DashboardTask['status']; label: string }> = [
  { value: 'todo', label: '未开始' },
  { value: 'in_progress', label: '进行中' },
  { value: 'done', label: '已完成' },
  { value: 'delayed', label: '延期' },
]
const taskOptions = ['制作视频', '制作封面', '标题简介', '发布视频'] as const

const weekdayOptions: Array<{ value: number; label: string }> = [
  { value: 1, label: '周一' },
  { value: 2, label: '周二' },
  { value: 3, label: '周三' },
  { value: 4, label: '周四' },
  { value: 5, label: '周五' },
  { value: 6, label: '周六' },
  { value: 0, label: '周日' },
]

type EditDraft = {
  taskId: string
  title: string
  channelId: string
  taskName: string
  priority: DashboardTask['priority']
  startTime: string
  endTime: string
  notes: string
  status: DashboardTask['status']
}

const labelOf = <T extends string>(value: T, list: Array<{ value: T; label: string }>, fallback: string) =>
  list.find((item) => item.value === value)?.label || fallback

const todayStatusLabel = (status: DashboardChannelOverview['today_status']) =>
  status === 'updated' ? '今日已更' : status === 'due' ? '应更未更' : '今日可不更'
const workflowStatusLabel = (status: DashboardChannelOverview['workflow_status']) =>
  status === 'blocked' ? '受阻' : status === 'paused' ? '暂停' : '推进中'
const formatTaskRange = (start?: string | null, end?: string | null) =>
  start && end ? `${start} 到 ${end}` : start ? `${start} 开始` : end ? `${end} 结束` : '未排时段'
const avatarFallback = (title: string) => (title.trim() ? title.trim().slice(0, 1).toUpperCase() : 'Y')
const weekdayOrder = weekdayOptions.map((item) => item.value)
const sortPublishDays = (days: number[]) => weekdayOrder.filter((day) => days.includes(day))
const resolvePublishDays = (channel: DashboardChannelOverview) => {
  const explicitDays = Array.isArray(channel.publish_days)
    ? channel.publish_days.filter((day) => Number.isInteger(day) && day >= 0 && day <= 6)
    : []
  if (explicitDays.length) return sortPublishDays(explicitDays)
  if (channel.update_cadence === 'daily') return sortPublishDays([1, 2, 3, 4, 5, 6, 0])
  if (channel.update_cadence === 'weekdays') return sortPublishDays([1, 2, 3, 4, 5])
  return []
}
const publishDaysSummary = (channel: DashboardChannelOverview) => {
  const days = resolvePublishDays(channel)
  if (days.length === 7) return '每天'
  if (days.length === 5 && [1, 2, 3, 4, 5].every((day) => days.includes(day))) return '工作日'
  if (!days.length) return '未设置'
  return weekdayOptions.filter((item) => days.includes(item.value)).map((item) => item.label).join(' ')
}
const deriveTodayStatusFromDays = (
  currentStatus: DashboardChannelOverview['today_status'],
  workflowStatus: DashboardChannelOverview['workflow_status'],
  date: string,
  cadence: string,
  publishDays: number[],
): DashboardChannelOverview['today_status'] => {
  if (currentStatus === 'updated') return 'updated'
  if (workflowStatus === 'paused') return 'optional'
  const weekday = new Date(`${date}T12:00:00Z`).getUTCDay()
  const dueToday =
    cadence === 'daily' ||
    (cadence === 'weekdays' && weekday >= 1 && weekday <= 5) ||
    ((cadence === 'weekly' || cadence === 'custom') && publishDays.includes(weekday))
  return dueToday ? 'due' : 'optional'
}

function Icon({ children }: { children: React.ReactNode }) {
  return <span className="dashboard-task-icon-wrap">{children}</span>
}

function todayInShanghaiClient() {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(new Date())
}

function shiftDate(date: string, diffDays: number) {
  const base = new Date(`${date}T00:00:00+08:00`)
  base.setUTCDate(base.getUTCDate() + diffDays)
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(base)
}

function EditIcon() {
  return (
    <Icon>
      <svg viewBox="0 0 20 20" fill="none" aria-hidden="true">
        <path d="M13.9 3.7a1.8 1.8 0 0 1 2.5 2.5l-8.1 8.1-3.2.7.7-3.2 8.1-8.1Z" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M12.5 5.1l2.4 2.4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    </Icon>
  )
}
function SaveIcon() {
  return (
    <Icon>
      <svg viewBox="0 0 20 20" fill="none" aria-hidden="true">
        <path d="M4.5 4.5h8l3 3v8a1 1 0 0 1-1 1h-10a1 1 0 0 1-1-1v-10a1 1 0 0 1 1-1Z" stroke="currentColor" strokeWidth="1.5" strokeLinejoin="round" />
        <path d="M7 4.5v4h5v-4" stroke="currentColor" strokeWidth="1.5" strokeLinejoin="round" />
      </svg>
    </Icon>
  )
}
function CancelIcon() {
  return (
    <Icon>
      <svg viewBox="0 0 20 20" fill="none" aria-hidden="true">
        <path d="M5 5l10 10M15 5L5 15" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
      </svg>
    </Icon>
  )
}
function TrashIcon() {
  return (
    <Icon>
      <svg viewBox="0 0 20 20" fill="none" aria-hidden="true">
        <path d="M4.5 5.5h11" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
        <path d="M8 3.5h4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
        <path d="M6.2 5.5l.6 9.1a1 1 0 0 0 1 .9h4.4a1 1 0 0 0 1-.9l.6-9.1" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    </Icon>
  )
}

export default function HomePage() {
  const [selectedDate, setSelectedDate] = useState(() => todayInShanghaiClient())
  const [dashboard, setDashboard] = useState<DashboardSummary | null>(null)
  const [loading, setLoading] = useState(true)
  const [errorText, setErrorText] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [taskTitle, setTaskTitle] = useState('')
  const [taskChannelId, setTaskChannelId] = useState('')
  const [taskName, setTaskName] = useState('')
  const [taskPriority, setTaskPriority] = useState<DashboardTask['priority']>('high')
  const [taskStartTime, setTaskStartTime] = useState('')
  const [taskEndTime, setTaskEndTime] = useState('')
  const [taskNotes, setTaskNotes] = useState('')
  const [taskPickerOpen, setTaskPickerOpen] = useState(false)
  const [editDraft, setEditDraft] = useState<EditDraft | null>(null)
  const [pendingDeleteTaskId, setPendingDeleteTaskId] = useState('')
  const [savingTaskId, setSavingTaskId] = useState('')
  const [mutatingTaskId, setMutatingTaskId] = useState('')
  const [mutatingChannelId, setMutatingChannelId] = useState('')
  const [frequencyEditorChannelId, setFrequencyEditorChannelId] = useState('')
  const [topTaskStatusMenuId, setTopTaskStatusMenuId] = useState('')
  const taskPickerRef = useRef<HTMLDivElement | null>(null)
  const frequencyEditorRef = useRef<HTMLDivElement | null>(null)
  const topTaskStatusRef = useRef<HTMLDivElement | null>(null)

  const loadDashboard = async (silent = false, date = selectedDate) => {
    if (!silent) setLoading(true)
    try {
      setDashboard(await api.getDashboard({ date }))
      setErrorText('')
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : '仪表盘加载失败')
    } finally {
      if (!silent) setLoading(false)
    }
  }

  useEffect(() => {
    void loadDashboard(false, selectedDate)
    const timer = window.setInterval(() => void loadDashboard(true, selectedDate), 15000)
    return () => window.clearInterval(timer)
  }, [selectedDate])

  useEffect(() => {
    const onDown = (event: MouseEvent) => {
      if (taskPickerRef.current && !taskPickerRef.current.contains(event.target as Node)) setTaskPickerOpen(false)
      if (frequencyEditorRef.current && !frequencyEditorRef.current.contains(event.target as Node)) setFrequencyEditorChannelId('')
      if (topTaskStatusRef.current && !topTaskStatusRef.current.contains(event.target as Node)) setTopTaskStatusMenuId('')
    }
    window.addEventListener('mousedown', onDown)
    return () => window.removeEventListener('mousedown', onDown)
  }, [])

  const channelOptions = useMemo(() => dashboard?.channel_overview || [], [dashboard?.channel_overview])
  const recentChannels = useMemo(() => {
    const rows = [...(dashboard?.channel_overview || [])]
    return rows.sort((a, b) => String(b.last_sync_at || '').localeCompare(String(a.last_sync_at || '')))
  }, [dashboard?.channel_overview])
  const filteredTaskOptions = useMemo(() => {
    const q = taskName.trim().toLowerCase()
    return q ? taskOptions.filter((item) => item.toLowerCase().includes(q)) : [...taskOptions]
  }, [taskName])
  const hasExactTaskOption = useMemo(() => taskOptions.some((item) => item === taskName.trim()), [taskName])

  const resetTaskForm = () => {
    setTaskTitle('')
    setTaskChannelId('')
    setTaskName('')
    setTaskPriority('high')
    setTaskStartTime('')
    setTaskEndTime('')
    setTaskNotes('')
    setTaskPickerOpen(false)
  }

  const validateTask = (title: string, name: string, start: string, end: string) => {
    if (!title.trim()) return '请先填写任务标题'
    if (!name.trim()) return '请先选择或输入任务'
    if ((start && !end) || (!start && end)) return '开始时间和结束时间需要一起填写'
    if (start && end && end <= start) return '结束时间必须晚于开始时间'
    return ''
  }

  const handleCreateTask = async () => {
    if (!dashboard) return
    const validationError = validateTask(taskTitle, taskName, taskStartTime, taskEndTime)
    if (validationError) return setErrorText(validationError)
    setSubmitting(true)
    try {
      await api.createDashboardTask({
        title: taskTitle.trim(),
        task_name: taskName.trim(),
        channel_id: taskChannelId || null,
        due_date: dashboard.date,
        priority: taskPriority,
        planned_start_time: taskStartTime || null,
        planned_end_time: taskEndTime || null,
        notes: taskNotes.trim() || null,
      })
      resetTaskForm()
      await loadDashboard(true)
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : '创建任务失败')
    } finally {
      setSubmitting(false)
    }
  }

  const beginEdit = (task: DashboardTask) =>
    setEditDraft({
      taskId: task.task_id,
      title: task.title || '',
      channelId: task.channel_id || '',
      taskName: task.task_name || '',
      priority: task.priority,
      startTime: task.planned_start_time || '',
      endTime: task.planned_end_time || '',
      notes: task.notes || '',
      status: task.status,
    })

  const persistTaskDraft = async (draft: EditDraft, closeAfter = false) => {
    if (!dashboard) return
    const validationError = validateTask(draft.title, draft.taskName, draft.startTime, draft.endTime)
    if (validationError) return setErrorText(validationError)
    setSavingTaskId(draft.taskId)
    try {
      await api.updateDashboardTask(draft.taskId, {
        title: draft.title.trim(),
        task_name: draft.taskName.trim(),
        channel_id: draft.channelId || null,
        due_date: dashboard.date,
        priority: draft.priority,
        planned_start_time: draft.startTime || null,
        planned_end_time: draft.endTime || null,
        notes: draft.notes.trim() || null,
        status: draft.status,
      })
      await loadDashboard(true, selectedDate)
      setEditDraft((current) => {
        if (!current || current.taskId !== draft.taskId) return current
        return closeAfter ? null : draft
      })
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : '修改任务失败')
    } finally {
      setSavingTaskId('')
    }
  }

  const updateEditDraft = (patch: Partial<EditDraft>, autoSave = false) => {
    setEditDraft((current) => {
      if (!current) return current
      const next = { ...current, ...patch }
      if (autoSave) void persistTaskDraft(next)
      return next
    })
  }

  const updateStatus = async (task: DashboardTask, status: DashboardTask['status']) => {
    setMutatingTaskId(task.task_id)
    try {
      await api.updateDashboardTask(task.task_id, { status })
      await loadDashboard(true)
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : '更新任务状态失败')
    } finally {
      setMutatingTaskId('')
    }
  }

  const deleteTask = async (taskId: string) => {
    setMutatingTaskId(taskId)
    try {
      await api.deleteDashboardTask(taskId)
      if (editDraft?.taskId === taskId) setEditDraft(null)
      await loadDashboard(true)
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : '删除任务失败')
    } finally {
      setMutatingTaskId('')
    }
  }

  const confirmDeleteTask = async () => {
    if (!pendingDeleteTaskId) return
    await deleteTask(pendingDeleteTaskId)
    setPendingDeleteTaskId('')
  }

  const togglePublishDay = async (channel: DashboardChannelOverview, dayValue: number) => {
    const currentDays = resolvePublishDays(channel)
    const nextDays = currentDays.includes(dayValue)
      ? currentDays.filter((item) => item !== dayValue)
      : sortPublishDays([...currentDays, dayValue])
    const cadence = nextDays.length === 7
      ? 'daily'
      : nextDays.length === 5 && [1, 2, 3, 4, 5].every((day) => nextDays.includes(day))
        ? 'weekdays'
        : nextDays.length > 0
          ? 'custom'
          : 'manual'
    setMutatingChannelId(channel.channel_id)
    try {
      await api.updateChannel(channel.channel_id, {
        sync_policy: {
          cadence,
          publish_days: nextDays,
          target_publish_time: channel.target_publish_time || null,
        },
      })
      setFrequencyEditorChannelId('')
      await loadDashboard(true, selectedDate)
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : '更新频道状态失败')
    } finally {
      setMutatingChannelId('')
    }
  }

  if (loading && !dashboard) {
    return <div className="card-flat dashboard-loading-card"><div className="empty-state"><div className="empty-state-title">仪表盘加载中</div>正在准备今日运营驾驶舱。</div></div>
  }
  if (!dashboard) {
    return <div className="card-flat dashboard-loading-card"><div className="empty-state"><div className="empty-state-title">仪表盘暂不可用</div>{errorText || '请稍后再试。'}</div></div>
  }

  return (
    <div className="dashboard-page">
      <div className="dashboard-hero">
        <div className="dashboard-hero-copy">
          <div className="dashboard-kicker">今日运营驾驶舱</div>
          <h1 className="dashboard-title">别总是脑子在开会，事情没有往前推一格</h1>
          <div className="dashboard-hero-meta dashboard-subtitle">
            <span className="dashboard-date-label">{`数据日期: ${dashboard.date}`}</span>
            <div className="dashboard-hero-actions">
              <div className="dashboard-date-switcher">
                <button type="button" className="btn btn-ghost btn-sm" onClick={() => setSelectedDate((value) => shiftDate(value, -1))}>
                  前一天
                </button>
                <input
                  className="input dashboard-date-input"
                  type="date"
                  value={selectedDate}
                  max={todayInShanghaiClient()}
                  onChange={(e) => setSelectedDate(e.target.value || todayInShanghaiClient())}
                />
                <button type="button" className="btn btn-ghost btn-sm" onClick={() => setSelectedDate((value) => shiftDate(value, 1))} disabled={selectedDate >= todayInShanghaiClient()}>
                  后一天
                </button>
                <button type="button" className="btn btn-secondary btn-sm" onClick={() => setSelectedDate(todayInShanghaiClient())} disabled={selectedDate === todayInShanghaiClient()}>
                  今天
                </button>
                <button className="btn btn-secondary btn-sm" onClick={() => void loadDashboard(false, selectedDate)} disabled={loading}>
                  {loading ? '刷新中...' : '刷新仪表盘'}
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>

      {errorText && <div className="tools-similar-issues">{errorText}</div>}

      <section className="dashboard-overview-grid">
        {[
          ['频道总数', dashboard.overview.channel_total, 'accent-purple'],
          ['推进中频道', dashboard.overview.active_channel_total, 'accent-blue'],
          ['今日应更频道', dashboard.overview.due_today_total, 'accent-orange'],
          ['今日已更频道', dashboard.overview.updated_today_total, 'accent-green'],
          ['今日计划数', dashboard.overview.task_total, 'accent-purple'],
          ['今日已完成计划', dashboard.overview.completed_task_total, 'accent-green'],
          ['今日执行进度', `${dashboard.overview.progress_percent}%`, 'accent-red'],
        ].map(([label, value, klass]) => (
          <article key={String(label)} className={`dashboard-metric-card ${klass}`}>
            <div className="dashboard-metric-label">{label}</div>
            <div className="dashboard-metric-value">{value}</div>
          </article>
        ))}
      </section>

      <section className="dashboard-secondary-grid dashboard-top-grid dashboard-dual-grid">
        <div className="dashboard-column">
          <section className="card-flat dashboard-panel">
            <div className="dashboard-panel-header"><div><h2>Todolist</h2><p>先处理最关键的 1 到 3 个任务。</p></div></div>
            <div className="dashboard-top-tasks dashboard-top-tasks-stack">
              {dashboard.top_tasks.length === 0 ? (
                <div className="dashboard-empty-inline">今天还没有关键任务。</div>
              ) : dashboard.top_tasks.map((task, index) => (
                <div key={task.task_id} className={`dashboard-top-task-card priority-${task.priority} status-${task.status}`}>
                  <div className="dashboard-top-task-rank">{`0${index + 1}`}</div>
                  <div className="dashboard-top-task-content">
                    <div className="dashboard-top-task-title">{task.title}</div>
                    <div className="dashboard-top-task-meta">
                      <div className="dashboard-top-task-meta-left">
                        <span className="dashboard-top-task-channel-label">{task.channel_title || '不关联频道'}</span>
                        <span className="dashboard-top-task-kind-label">{task.task_name || '未分类任务'}</span>
                        <span className={`dashboard-priority-badge priority-${task.priority}`}>{labelOf(task.priority, priorityOptions, '中')}</span>
                      </div>
                      <div className="dashboard-top-task-meta-right">
                        <span className="dashboard-top-task-time-label">{formatTaskRange(task.planned_start_time, task.planned_end_time)}</span>
                        <div
                          className="dashboard-top-task-status-menu"
                          ref={topTaskStatusMenuId === task.task_id ? topTaskStatusRef : null}
                        >
                          <button
                            type="button"
                            className={`dashboard-top-task-status-trigger status-${task.status}${topTaskStatusMenuId === task.task_id ? ' open' : ''}`}
                            onClick={() => setTopTaskStatusMenuId((value) => value === task.task_id ? '' : task.task_id)}
                            disabled={mutatingTaskId === task.task_id || !!editDraft}
                          >
                            <span className="dashboard-top-task-status-dot" />
                            <span>{labelOf(task.status, statusOptions, '未开始')}</span>
                          </button>
                          {topTaskStatusMenuId === task.task_id ? (
                            <div className="dashboard-top-task-status-dropdown">
                              {statusOptions.map((item) => (
                                <button
                                  key={item.value}
                                  type="button"
                                  className={`dashboard-top-task-status-option status-${item.value}${task.status === item.value ? ' active' : ''}`}
                                  onClick={() => {
                                    setTopTaskStatusMenuId('')
                                    void updateStatus(task, item.value)
                                  }}
                                  disabled={mutatingTaskId === task.task_id}
                                >
                                  <span className="dashboard-top-task-status-dot" />
                                  <span>{item.label}</span>
                                </button>
                              ))}
                            </div>
                          ) : null}
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </section>
        </div>

        <div className="dashboard-column">
          <div className="card-flat dashboard-panel">
            <div className="dashboard-panel-header"><div><h2>执行进度 / 系统提醒</h2><p>盯完成率、任务队列和同步异常。</p></div></div>
            <div className="dashboard-progress-card">
              <div className="dashboard-progress-meta">
                <div className="dashboard-progress-value">{`${dashboard.overview.progress_percent}%`}</div>
                <div className="dashboard-progress-copy">
                  <div>{`已完成 ${dashboard.overview.completed_task_total} / ${dashboard.overview.task_total} 项计划`}</div>
                  <div>{`运行中任务 ${dashboard.monitoring.running_jobs} 个，排队 ${dashboard.monitoring.queued_jobs} 个`}</div>
                </div>
                <div className="dashboard-monitor-cards dashboard-monitor-cards-inline">
                  <div className="dashboard-mini-monitor"><span>运行中</span><strong>{dashboard.monitoring.running_jobs}</strong></div>
                  <div className="dashboard-mini-monitor"><span>排队中</span><strong>{dashboard.monitoring.queued_jobs}</strong></div>
                  <div className="dashboard-mini-monitor"><span>失败任务</span><strong>{dashboard.monitoring.failed_jobs}</strong></div>
                </div>
              </div>
              <div className="dashboard-progress-track"><div className="dashboard-progress-fill" style={{ width: `${dashboard.overview.progress_percent}%` }} /></div>
            </div>
            <div className="dashboard-reminder-list">
              {dashboard.monitoring.reminders.map((item, index) => (
                <div key={`${item.title}-${index}`} className={`dashboard-reminder ${item.level}`}>
                  <div className="dashboard-reminder-title">{item.title}</div>
                  <div className="dashboard-reminder-detail">{item.detail}</div>
                </div>
              ))}
            </div>
            <div className="dashboard-failed-syncs">
              <div className="dashboard-subsection-title">最近同步失败</div>
              {dashboard.monitoring.recent_failed_syncs.length === 0 ? <div className="dashboard-empty-inline">最近没有同步失败。</div> : dashboard.monitoring.recent_failed_syncs.map((item) => (
                <div key={item.job_id} className="dashboard-failed-item">
                  <div><strong>{item.type}</strong><span>{item.created_at ? relTime(item.created_at) : '刚刚'}</span></div>
                  <div>{item.error_message || '未知错误'}</div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section className="dashboard-plan-row">
        <div className="card-flat dashboard-panel dashboard-plan-panel">
          <div className="dashboard-panel-header"><div><h2>每日计划</h2><p>先抓关键任务，再按频道推进执行。</p></div></div>
          <div className="dashboard-task-form dashboard-task-form-rich">
            <input className="input" value={taskTitle} onChange={(e) => setTaskTitle(e.target.value)} placeholder="任务标题" />
            <select className="input" value={taskChannelId} onChange={(e) => setTaskChannelId(e.target.value)}>
              <option value="">不关联频道</option>
              {channelOptions.map((channel) => <option key={channel.channel_id} value={channel.channel_id}>{channel.title}</option>)}
            </select>
            <div className={`dashboard-task-picker ${taskPickerOpen ? 'is-open' : ''}`} ref={taskPickerRef}>
              <input className="input dashboard-task-picker-input" value={taskName} onChange={(e) => { setTaskName(e.target.value); setTaskPickerOpen(true) }} onFocus={() => setTaskPickerOpen(true)} placeholder="选择或输入任务" />
              <button type="button" className="dashboard-task-picker-toggle" onClick={() => setTaskPickerOpen((value) => !value)} aria-label="切换任务选项">▾</button>
              {taskPickerOpen && (
                <div className="dashboard-task-picker-menu">
                  {filteredTaskOptions.map((item) => <button key={item} type="button" className={`dashboard-task-picker-option ${taskName === item ? 'active' : ''}`} onClick={() => { setTaskName(item); setTaskPickerOpen(false) }}>{item}</button>)}
                  {taskName.trim() && !hasExactTaskOption && <button type="button" className="dashboard-task-picker-option dashboard-task-picker-option-create" onClick={() => setTaskPickerOpen(false)}>{`使用“${taskName.trim()}”`}</button>}
                  {!filteredTaskOptions.length && !taskName.trim() && <div className="dashboard-task-picker-empty">暂无任务模板</div>}
                </div>
              )}
            </div>
            <select className="input" value={taskPriority} onChange={(e) => setTaskPriority(e.target.value as DashboardTask['priority'])}>
              {priorityOptions.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}
            </select>
            <div className="dashboard-task-time-range">
              <input className="input" type="time" step="60" value={taskStartTime} onChange={(e) => setTaskStartTime(e.target.value)} />
              <span>到</span>
              <input className="input" type="time" step="60" value={taskEndTime} onChange={(e) => setTaskEndTime(e.target.value)} />
            </div>
            <input className="input dashboard-task-notes" value={taskNotes} onChange={(e) => setTaskNotes(e.target.value)} placeholder="备注，可留空" />
            <div className="dashboard-task-actions-inline">
              <button className="btn btn-primary dashboard-task-submit" onClick={() => void handleCreateTask()} disabled={submitting}>{submitting ? '添加中...' : '添加今日计划'}</button>
            </div>
          </div>

          {dashboard.tasks.length === 0 ? (
            <div className="dashboard-empty-inline">今天还没有计划项，先把任务补进来。</div>
          ) : (
            <div className="table-container dashboard-task-table-container">
              <table className="dashboard-task-table">
                <thead>
                  <tr>
                    <th className="dashboard-avatar-col">头像</th>
                    <th>频道名称</th>
                    <th>任务标题</th>
                    <th>任务</th>
                    <th>优先级</th>
                    <th>执行时间</th>
                    <th>备注</th>
                    <th>状态</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
{dashboard.tasks.map((task) => {
                    const editing = editDraft?.taskId === task.task_id
                    return (
                      <tr key={task.task_id} className={editing ? 'is-editing' : undefined}>
                        <td>{task.channel_avatar_url ? <img className="dashboard-channel-avatar" src={task.channel_avatar_url} alt={task.channel_title || ''} /> : <div className="dashboard-channel-avatar dashboard-channel-avatar-fallback">{avatarFallback(task.channel_title || '任务')}</div>}</td>
                        <td>{editing ? (
                          <select className="input dashboard-inline-input dashboard-inline-select" value={editDraft.channelId} onChange={(e) => updateEditDraft({ channelId: e.target.value }, true)}>
                            <option value="">不关联频道</option>
                            {channelOptions.map((channel) => <option key={channel.channel_id} value={channel.channel_id}>{channel.title}</option>)}
                          </select>
                        ) : (task.channel_title || '不关联频道')}</td>
                        <td>{editing ? <input className="input dashboard-inline-input" value={editDraft.title} onChange={(e) => updateEditDraft({ title: e.target.value })} onBlur={() => void persistTaskDraft(editDraft)} /> : <div className="dashboard-task-title-ellipsis" title={task.title}>{task.title}</div>}</td>
                        <td>{editing ? <input className="input dashboard-inline-input" value={editDraft.taskName} onChange={(e) => updateEditDraft({ taskName: e.target.value })} onBlur={() => void persistTaskDraft(editDraft)} /> : (task.task_name || '未分类任务')}</td>
                        <td>{editing ? (
                          <select className="input dashboard-inline-input dashboard-inline-select" value={editDraft.priority} onChange={(e) => updateEditDraft({ priority: e.target.value as DashboardTask['priority'] }, true)}>
                            {priorityOptions.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}
                          </select>
                        ) : <span className={`dashboard-priority-badge priority-${task.priority}`}>{labelOf(task.priority, priorityOptions, '中')}</span>}</td>
                        <td>{editing ? (
                          <div className="dashboard-inline-time-range">
                            <input className="input dashboard-inline-input" type="time" step="60" value={editDraft.startTime} onChange={(e) => updateEditDraft({ startTime: e.target.value })} onBlur={() => void persistTaskDraft(editDraft)} />
                            <input className="input dashboard-inline-input" type="time" step="60" value={editDraft.endTime} onChange={(e) => updateEditDraft({ endTime: e.target.value })} onBlur={() => void persistTaskDraft(editDraft)} />
                          </div>
                        ) : formatTaskRange(task.planned_start_time, task.planned_end_time)}</td>
                        <td>{editing ? <input className="input dashboard-inline-input" value={editDraft.notes} onChange={(e) => updateEditDraft({ notes: e.target.value })} onBlur={() => void persistTaskDraft(editDraft)} /> : <div className="dashboard-task-notes-ellipsis" title={task.notes || ''}>{task.notes || '-'}</div>}</td>
                        <td>{editing ? (
                            <select className="input dashboard-task-status-select dashboard-inline-select" value={editDraft.status} onChange={(e) => updateEditDraft({ status: e.target.value as DashboardTask['status'] }, true)}>
                            {statusOptions.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}
                          </select>
                        ) : (
                          <select className="input dashboard-task-status-select" value={task.status} onChange={(e) => void updateStatus(task, e.target.value as DashboardTask['status'])} disabled={mutatingTaskId === task.task_id}>
                            {statusOptions.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}
                          </select>
                        )}</td>
                        <td>
                          <div className="dashboard-task-operation-cell">
                            {editing ? (
                              <>
                                <button type="button" className="dashboard-task-icon-btn" onClick={() => setEditDraft(null)} disabled={savingTaskId === task.task_id} title="取消" aria-label="取消"><CancelIcon /></button>
                              </>
                            ) : (
                              <button type="button" className="dashboard-task-icon-btn" onClick={() => beginEdit(task)} disabled={!!editDraft || mutatingTaskId === task.task_id} title="编辑" aria-label="编辑"><EditIcon /></button>
                            )}
                            <button type="button" className="dashboard-task-icon-btn danger" onClick={() => setPendingDeleteTaskId(task.task_id)} disabled={mutatingTaskId === task.task_id || savingTaskId === task.task_id} title="删除" aria-label="删除"><TrashIcon /></button>
                          </div>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </section>

      <section className="dashboard-channel-row">
        <section className="card-flat dashboard-panel">
          <div className="dashboard-panel-header"><div><h2>频道更新概览</h2><p>优先看今日应更、最近更新和最近同步的频道。</p></div></div>
          <div className="dashboard-channel-overview-full">
            <table>
              <thead>
                <tr>
                  <th className="dashboard-avatar-col">头像</th>
                  <th>频道名称</th>
                  <th>当前状态</th>
                  <th className="dashboard-video-thumb-col">最近视频封面</th>
                  <th>最近视频标题</th>
                  <th>最近视频发布时间</th>
                  <th className="dashboard-frequency-col">更新频率</th>
                  <th>今日状态</th>
                  <th>最近同步时间</th>
                </tr>
              </thead>
              <tbody>
                {recentChannels.length === 0 ? (
                  <tr><td colSpan={9} style={{ color: 'var(--text-secondary)' }}>暂无频道数据</td></tr>
                ) : recentChannels.map((channel) => (
                  <tr key={channel.channel_id}>
                    <td>{channel.avatar_url ? <img className="dashboard-channel-avatar" src={channel.avatar_url} alt={channel.title} /> : <div className="dashboard-channel-avatar dashboard-channel-avatar-fallback">{avatarFallback(channel.title)}</div>}</td>
                    <td>{channel.title}</td>
                    <td><span className={`dashboard-channel-status status-${channel.workflow_status}`}>{workflowStatusLabel(channel.workflow_status)}</span></td>
                    <td>{channel.latest_video_thumbnail_url ? <img className="dashboard-video-thumb" src={channel.latest_video_thumbnail_url} alt={channel.latest_video_title || '最近视频封面'} loading="lazy" /> : <div className="dashboard-video-thumb dashboard-video-thumb-fallback">暂无</div>}</td>
                    <td><div className="dashboard-video-title-ellipsis dashboard-video-title-compact">{channel.latest_video_title || '暂无视频'}</div></td>
                    <td>{channel.latest_video_published_at ? formatDateTime(channel.latest_video_published_at) : 'N/A'}</td>
                    <td>
                      <div
                        className="dashboard-frequency-wrap"
                        ref={frequencyEditorChannelId === channel.channel_id ? frequencyEditorRef : null}
                      >
                        <button
                          type="button"
                          className={`dashboard-frequency-trigger${frequencyEditorChannelId === channel.channel_id ? ' open' : ''}`}
                          onClick={() => setFrequencyEditorChannelId((value) => value === channel.channel_id ? '' : channel.channel_id)}
                          disabled={mutatingChannelId === channel.channel_id}
                        >
                          <span className={`dashboard-frequency-summary${resolvePublishDays(channel).length ? ' has-value' : ''}`}>
                            {publishDaysSummary(channel)}
                          </span>
                          <span className="dashboard-frequency-trigger-meta">{frequencyEditorChannelId === channel.channel_id ? '收起' : '编辑'}</span>
                        </button>
                        {frequencyEditorChannelId === channel.channel_id ? (
                          <div className="dashboard-frequency-popover">
                            <div className="dashboard-frequency-cell">
                              {weekdayOptions.map((option) => {
                                const selectedDays = resolvePublishDays(channel)
                                const active = selectedDays.includes(option.value)
                                return (
                                  <button
                                    key={`${channel.channel_id}-${option.value}`}
                                    type="button"
                                    className={`dashboard-frequency-chip${active ? ' active' : ''}`}
                                    onClick={() => void togglePublishDay(channel, option.value)}
                                    disabled={mutatingChannelId === channel.channel_id}
                                    aria-pressed={active}
                                  >
                                    {option.label}
                                  </button>
                                )
                              })}
                            </div>
                          </div>
                        ) : null}
                      </div>
                    </td>
                    <td><span className={`dashboard-today-status status-${channel.today_status}`}>{todayStatusLabel(channel.today_status)}</span></td>
                    <td>{channel.last_sync_at ? relTime(channel.last_sync_at) : '从未同步'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </section>

      {pendingDeleteTaskId ? (
        <div className="dashboard-confirm-overlay" role="presentation">
          <div className="dashboard-confirm-modal" role="dialog" aria-modal="true" aria-labelledby="task-delete-title">
            <h3 id="task-delete-title">确认删除计划</h3>
            <p>删除后无法撤销，确认继续吗？</p>
            <div className="dashboard-confirm-actions">
              <button type="button" className="btn btn-secondary" onClick={() => setPendingDeleteTaskId('')} disabled={mutatingTaskId === pendingDeleteTaskId}>
                取消
              </button>
              <button type="button" className="btn btn-danger" onClick={() => void confirmDeleteTask()} disabled={mutatingTaskId === pendingDeleteTaskId}>
                确认删除
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  )
}
