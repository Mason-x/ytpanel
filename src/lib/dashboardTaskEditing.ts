import type { DashboardTask } from '../types'

export type TaskEditDraft = {
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

const padClockUnit = (value: number) => String(value).padStart(2, '0')

export function sanitizeTaskTimeSegment(value: string): string {
  return String(value || '').replace(/\D/g, '').slice(0, 2)
}

export function normalizeTaskTimeSegment(value: string, unit: 'hours' | 'minutes'): string {
  const text = sanitizeTaskTimeSegment(value)
  if (!text) return ''
  const numeric = Number(text)
  if (!Number.isInteger(numeric)) return ''
  if (unit === 'hours' && (numeric < 0 || numeric > 23)) return ''
  if (unit === 'minutes' && (numeric < 0 || numeric > 59)) return ''
  return padClockUnit(numeric)
}

export function splitTaskTimeText(value: string | null | undefined): { hours: string; minutes: string } {
  const normalized = normalizeTaskTimeText(value)
  if (!normalized) return { hours: '', minutes: '' }
  const [hours, minutes] = normalized.split(':', 2)
  return { hours, minutes }
}

export function buildTaskTimeText(hours: string, minutes: string): string {
  const normalizedHours = normalizeTaskTimeSegment(hours, 'hours')
  const normalizedMinutes = normalizeTaskTimeSegment(minutes, 'minutes')
  if (!normalizedHours || !normalizedMinutes) return ''
  return `${normalizedHours}:${normalizedMinutes}`
}

export function normalizeTaskTimeText(value: string | null | undefined): string {
  const text = String(value || '').trim()
  if (!text) return ''

  let hoursText = ''
  let minutesText = ''
  if (text.includes(':')) {
    const [hoursPart = '', minutesPart = ''] = text.split(':', 2)
    hoursText = hoursPart.trim()
    minutesText = minutesPart.trim()
  } else if (/^\d{3,4}$/.test(text)) {
    const normalized = text.padStart(4, '0')
    hoursText = normalized.slice(0, 2)
    minutesText = normalized.slice(2, 4)
  } else {
    return ''
  }

  if (!/^\d{1,2}$/.test(hoursText) || !/^\d{1,2}$/.test(minutesText)) return ''

  const hours = Number(hoursText)
  const minutes = Number(minutesText)
  if (!Number.isInteger(hours) || !Number.isInteger(minutes)) return ''
  if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59) return ''

  return `${padClockUnit(hours)}:${padClockUnit(minutes)}`
}

export function buildTaskEditDraft(task: DashboardTask): TaskEditDraft {
  return {
    taskId: task.task_id,
    title: task.title || '',
    channelId: task.channel_id || '',
    taskName: task.task_name || '',
    priority: task.priority,
    startTime: normalizeTaskTimeText(task.planned_start_time),
    endTime: normalizeTaskTimeText(task.planned_end_time),
    notes: task.notes || '',
    status: task.status,
  }
}
