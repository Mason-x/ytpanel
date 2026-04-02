import { useEffect, useState, type KeyboardEvent } from 'react'
import {
  buildTaskTimeText,
  normalizeTaskTimeSegment,
  splitTaskTimeText,
  sanitizeTaskTimeSegment,
} from '../../lib/dashboardTaskEditing'

type TaskActionButtonsProps = {
  editing: boolean
  disableEdit: boolean
  disableSave: boolean
  disableDelete: boolean
  onEdit: () => void
  onSave: () => void
  onCancel: () => void
  onDelete: () => void
}

type TaskTimeInputProps = {
  className?: string
  value: string
  disabled?: boolean
  'aria-label'?: string
  onChange: (value: string) => void
  onBlur?: () => void
  onEnter?: () => void
}

export function TaskActionButtons({
  editing,
  disableDelete,
  disableEdit,
  disableSave,
  onCancel,
  onDelete,
  onEdit,
  onSave,
}: TaskActionButtonsProps) {
  return (
    <div className="dashboard-task-operation-cell">
      {editing ? (
        <>
          <button type="button" className="btn btn-primary btn-sm dashboard-task-op-btn" onClick={onSave} disabled={disableSave}>
            保存
          </button>
          <button type="button" className="btn btn-ghost btn-sm dashboard-task-op-btn" onClick={onCancel} disabled={disableSave}>
            取消
          </button>
        </>
      ) : (
        <button type="button" className="btn btn-secondary btn-sm dashboard-task-op-btn" onClick={onEdit} disabled={disableEdit}>
          编辑
        </button>
      )}
      <button type="button" className="btn btn-secondary btn-sm dashboard-task-op-btn dashboard-task-op-btn-danger" onClick={onDelete} disabled={disableDelete}>
        删除
      </button>
    </div>
  )
}

export function TaskTimeInput({
  className,
  value,
  disabled,
  onChange,
  onBlur,
  onEnter,
  ...rest
}: TaskTimeInputProps) {
  const initialParts = splitTaskTimeText(value)
  const [hours, setHours] = useState(initialParts.hours)
  const [minutes, setMinutes] = useState(initialParts.minutes)

  useEffect(() => {
    const next = splitTaskTimeText(value)
    setHours(next.hours)
    setMinutes(next.minutes)
  }, [value])

  const handleHoursChange = (nextValue: string) => {
    const nextHours = sanitizeTaskTimeSegment(nextValue)
    setHours(nextHours)
    onChange(buildTaskTimeText(nextHours, minutes))
  }

  const handleMinutesChange = (nextValue: string) => {
    const nextMinutes = sanitizeTaskTimeSegment(nextValue)
    setMinutes(nextMinutes)
    onChange(buildTaskTimeText(hours, nextMinutes))
  }

  const handleBlur = () => {
    const normalizedHours = normalizeTaskTimeSegment(hours, 'hours')
    const normalizedMinutes = normalizeTaskTimeSegment(minutes, 'minutes')
    setHours(normalizedHours)
    setMinutes(normalizedMinutes)
    onChange(buildTaskTimeText(normalizedHours, normalizedMinutes))
    onBlur?.()
  }

  const handleKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Enter') onEnter?.()
  }

  return (
    <div className="dashboard-task-time-control">
      <input
        {...rest}
        className={`${className || ''} dashboard-task-time-segment`.trim()}
        type="text"
        inputMode="numeric"
        autoComplete="off"
        placeholder="HH"
        maxLength={2}
        value={hours}
        disabled={disabled}
        onChange={(event) => handleHoursChange(event.target.value)}
        onBlur={handleBlur}
        onKeyDown={handleKeyDown}
      />
      <span className="dashboard-task-time-separator" aria-hidden="true">:</span>
      <input
        className={`${className || ''} dashboard-task-time-segment`.trim()}
        type="text"
        inputMode="numeric"
        autoComplete="off"
        placeholder="MM"
        maxLength={2}
        value={minutes}
        disabled={disabled}
        aria-label={rest['aria-label'] ? `${rest['aria-label']}分钟` : undefined}
        onChange={(event) => handleMinutesChange(event.target.value)}
        onBlur={handleBlur}
        onKeyDown={handleKeyDown}
      />
    </div>
  )
}
