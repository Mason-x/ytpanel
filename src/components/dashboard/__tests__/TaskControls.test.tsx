import test from 'node:test'
import assert from 'node:assert/strict'
import { renderToStaticMarkup } from 'react-dom/server'
import { TaskActionButtons, TaskTimeInput } from '../TaskControls'

test('TaskActionButtons renders a visible save action while editing', () => {
  const html = renderToStaticMarkup(
    <TaskActionButtons
      editing
      disableDelete={false}
      disableEdit={false}
      disableSave={false}
      onCancel={() => {}}
      onDelete={() => {}}
      onEdit={() => {}}
      onSave={() => {}}
    />,
  )

  assert.match(html, /保存/)
  assert.match(html, /取消/)
  assert.match(html, /删除/)
})

test('TaskTimeInput renders a stable HH:MM text input', () => {
  const html = renderToStaticMarkup(
    <TaskTimeInput
      className="input"
      value="13:05"
      onChange={() => {}}
    />,
  )

  assert.match(html, /dashboard-task-time-segment/)
  assert.match(html, /type="text"/)
  assert.match(html, /placeholder="HH"/)
  assert.match(html, /placeholder="MM"/)
  assert.match(html, /inputMode="numeric"|inputmode="numeric"/)
  assert.match(html, /value="13"/)
  assert.match(html, /value="05"/)
  assert.match(html, /dashboard-task-time-separator/)
  assert.match(html, />:</)
})
