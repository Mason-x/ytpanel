export default function AddChannelModal({
  open,
  value,
  errorText,
  adding,
  onChange,
  onClose,
  onSubmit,
}: {
  open: boolean
  value: string
  errorText: string
  adding: boolean
  onChange: (value: string) => void
  onClose: () => void
  onSubmit: () => void
}) {
  if (!open) return null

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={(event) => event.stopPropagation()}>
        <div className="modal-title">添加频道</div>
        <form
          className="settings-form"
          onSubmit={(event) => {
            event.preventDefault()
            onSubmit()
          }}
        >
          <div className="form-group">
            <label className="form-label">YouTube 频道链接 / @Handle / Channel ID</label>
            <input
              className="input"
              value={value}
              onChange={(event) => onChange(event.target.value)}
              placeholder="https://www.youtube.com/@..."
              autoFocus
            />
          </div>

          {errorText && <div className="tools-similar-issues">{errorText}</div>}

          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
            <button type="button" className="btn btn-secondary" onClick={onClose}>
              取消
            </button>
            <button type="submit" className="btn btn-primary" disabled={adding}>
              {adding ? '添加中...' : '添加并同步'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
