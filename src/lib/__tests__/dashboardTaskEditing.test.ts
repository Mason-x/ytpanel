import test from 'node:test'
import assert from 'node:assert/strict'
import type { DashboardTask } from '../../types'
import {
  buildTaskEditDraft,
  buildTaskTimeText,
  normalizeTaskTimeText,
  splitTaskTimeText,
} from '../dashboardTaskEditing'

const sampleTask: DashboardTask = {
  task_id: 'task-1',
  title: '制作视频',
  task_name: '制作视频',
  due_date: '2026-03-27',
  priority: 'high',
  status: 'todo',
  planned_start_time: '09:30',
  planned_end_time: '11:00',
}

test('buildTaskEditDraft preserves planned times from task', () => {
  const draft = buildTaskEditDraft(sampleTask)

  assert.equal(draft.startTime, '09:30')
  assert.equal(draft.endTime, '11:00')
})

test('normalizeTaskTimeText normalizes valid 24-hour values', () => {
  assert.equal(normalizeTaskTimeText('9:5'), '09:05')
  assert.equal(normalizeTaskTimeText('23:59'), '23:59')
  assert.equal(normalizeTaskTimeText('24:00'), '')
})

test('splitTaskTimeText returns fixed hour and minute parts', () => {
  assert.deepEqual(splitTaskTimeText('09:30'), { hours: '09', minutes: '30' })
  assert.deepEqual(splitTaskTimeText(''), { hours: '', minutes: '' })
})

test('buildTaskTimeText joins hour and minute parts into HH:MM', () => {
  assert.equal(buildTaskTimeText('9', '5'), '09:05')
  assert.equal(buildTaskTimeText('09', ''), '')
  assert.equal(buildTaskTimeText('24', '00'), '')
})
