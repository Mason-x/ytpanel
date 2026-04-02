import test from 'node:test';
import assert from 'node:assert/strict';
import {
  buildDueChannelAutoTasks,
  type DashboardAutoTaskChannel,
  type DashboardAutoTaskRow,
} from '../dashboardAutoTasks';

const dueChannel: DashboardAutoTaskChannel = {
  channel_id: 'UC123',
  title: 'Alpha',
  today_status: 'due',
};

test('buildDueChannelAutoTasks creates a high priority publish task for due channels', () => {
  const tasks = buildDueChannelAutoTasks([dueChannel], [], '2026-03-29');

  assert.equal(tasks.length, 1);
  assert.deepEqual(tasks[0], {
    title: 'Alpha 发布视频',
    task_name: '发布视频',
    channel_id: 'UC123',
    due_date: '2026-03-29',
    priority: 'high',
    status: 'todo',
    planned_start_time: '00:00',
    planned_end_time: '23:59',
    notes: null,
    sort_order: 0,
  });
});

test('buildDueChannelAutoTasks skips duplicate publish task for the same channel and day', () => {
  const existingTasks: DashboardAutoTaskRow[] = [
    {
      task_id: 'task-1',
      task_name: '发布视频',
      channel_id: 'UC123',
      due_date: '2026-03-29',
    },
  ];

  const tasks = buildDueChannelAutoTasks([dueChannel], existingTasks, '2026-03-29');

  assert.equal(tasks.length, 0);
});

test('buildDueChannelAutoTasks ignores non-due channels', () => {
  const tasks = buildDueChannelAutoTasks([
    { channel_id: 'UC123', title: 'Alpha', today_status: 'optional' },
    { channel_id: 'UC456', title: 'Beta', today_status: 'updated' },
  ], [], '2026-03-29');

  assert.equal(tasks.length, 0);
});
