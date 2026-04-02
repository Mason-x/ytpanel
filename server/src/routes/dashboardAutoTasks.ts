export type DashboardAutoTaskChannel = {
  channel_id: string;
  title: string;
  today_status: 'updated' | 'due' | 'optional';
};

export type DashboardAutoTaskRow = {
  task_id?: string;
  task_name?: string | null;
  channel_id?: string | null;
  due_date?: string | null;
};

export type DashboardAutoTaskInsert = {
  title: string;
  task_name: string;
  channel_id: string;
  due_date: string;
  priority: 'high';
  status: 'todo';
  planned_start_time: '00:00';
  planned_end_time: '23:59';
  notes: null;
  sort_order: 0;
};

const AUTO_TASK_NAME = '发布视频';

export function buildDueChannelAutoTasks(
  channels: DashboardAutoTaskChannel[],
  existingTasks: DashboardAutoTaskRow[],
  date: string,
): DashboardAutoTaskInsert[] {
  const existingKeys = new Set(
    existingTasks
      .filter((task) => String(task.due_date || '').trim() === date)
      .map((task) => `${String(task.channel_id || '').trim()}::${String(task.task_name || '').trim()}`),
  );

  return channels
    .filter((channel) => channel.today_status === 'due')
    .filter((channel) => !existingKeys.has(`${channel.channel_id}::${AUTO_TASK_NAME}`))
    .map((channel) => ({
      title: `${channel.title} ${AUTO_TASK_NAME}`,
      task_name: AUTO_TASK_NAME,
      channel_id: channel.channel_id,
      due_date: date,
      priority: 'high',
      status: 'todo',
      planned_start_time: '00:00',
      planned_end_time: '23:59',
      notes: null,
      sort_order: 0,
    }));
}
