import type { KeyboardEvent } from 'react'

type TagStat = { tag: string; count: number }

export default function TagEditorModal({
  open,
  tagDraft,
  tagDraftInput,
  allTagStats,
  tagInputSuggestions,
  tagSuggestionFocusIndex,
  tagEditError,
  deletingTag,
  onClose,
  onInputChange,
  onInputKeyDown,
  onAddTag,
  onRemoveDraftTag,
  onToggleDraftTag,
  onSelectSuggestion,
  onDeleteExistingTag,
  onSave,
}: {
  open: boolean
  tagDraft: string[]
  tagDraftInput: string
  allTagStats: TagStat[]
  tagInputSuggestions: TagStat[]
  tagSuggestionFocusIndex: number
  tagEditError: string
  deletingTag: string
  onClose: () => void
  onInputChange: (value: string) => void
  onInputKeyDown: (event: KeyboardEvent<HTMLInputElement>) => void
  onAddTag: () => void
  onRemoveDraftTag: (tag: string) => void
  onToggleDraftTag: (tag: string) => void
  onSelectSuggestion: (tag: string) => void
  onDeleteExistingTag: (tag: string) => void
  onSave: () => void
}) {
  if (!open) return null

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal modal-tags" onClick={(event) => event.stopPropagation()}>
        <div className="modal-title">编辑标签</div>

        <div className="tag-modal-section">
          <div className="tag-modal-label">当前频道标签（支持多选）</div>
          <div className="tag-modal-chips">
            {tagDraft.length === 0 ? (
              <span className="tag-modal-empty">暂无标签</span>
            ) : (
              tagDraft.map((tag) => (
                <button key={`draft-${tag}`} type="button" className="tag-chip" onClick={() => onRemoveDraftTag(tag)}>
                  {`#${tag}`} <span aria-hidden="true">×</span>
                </button>
              ))
            )}
          </div>
        </div>

        <div className="tag-modal-section">
          <div className="tag-modal-label">新增标签（用逗号分隔，可一次添加多个）</div>
          <div className="tag-input-row">
            <input
              className="input"
              value={tagDraftInput}
              onChange={(event) => onInputChange(event.target.value)}
              onKeyDown={onInputKeyDown}
              autoFocus
              placeholder="输入标签后回车或点击添加"
            />
            <button type="button" className="btn btn-secondary" onClick={onAddTag}>
              添加
            </button>
          </div>

          {tagInputSuggestions.length > 0 && (
            <div className="tag-suggestion-list">
              {tagInputSuggestions.map((item, index) => {
                const active = index === (tagSuggestionFocusIndex >= 0 ? tagSuggestionFocusIndex : 0)
                return (
                  <button
                    key={`suggest-${item.tag}`}
                    type="button"
                    onMouseDown={(event) => {
                      event.preventDefault()
                      onSelectSuggestion(item.tag)
                    }}
                    className={`tag-suggestion-item ${active ? 'active' : ''}`}
                  >
                    <span>{`#${item.tag}`}</span>
                    <span>{`${item.count} 个频道`}</span>
                  </button>
                )
              })}
            </div>
          )}
        </div>

        <div className="tag-modal-section">
          <div className="tag-modal-label">已有标签（点击选择/取消）</div>
          <div className="tag-modal-chips scrollable">
            {allTagStats.length === 0 ? (
              <span className="tag-modal-empty">暂无可选标签</span>
            ) : (
              allTagStats.map((item) => {
                const active = tagDraft.includes(item.tag)
                return (
                  <button
                    key={`pool-${item.tag}`}
                    type="button"
                    className={`tag-chip tag-chip-pool ${active ? 'active' : ''}`}
                    onClick={() => onToggleDraftTag(item.tag)}
                  >
                    {`#${item.tag} (${item.count})`}
                  </button>
                )
              })
            )}
          </div>
        </div>

        <div className="tag-modal-section">
          <div className="tag-modal-label">标签管理（删除已有标签）</div>
          <div className="tag-manage-list">
            {allTagStats.length === 0 ? (
              <span className="tag-modal-empty">暂无可删除标签</span>
            ) : (
              allTagStats.map((item) => (
                <button
                  key={`manage-${item.tag}`}
                  type="button"
                  className="tag-manage-chip"
                  onMouseDown={(event) => event.preventDefault()}
                  onClick={() => onDeleteExistingTag(item.tag)}
                  disabled={deletingTag === item.tag}
                >
                  <span aria-hidden="true" className="tag-manage-icon">
                    <svg
                      width="14"
                      height="14"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    >
                      <path d="M3 6h18" />
                      <path d="M8 6V4h8v2" />
                      <path d="M19 6l-1 14H6L5 6" />
                      <path d="M10 11v6" />
                      <path d="M14 11v6" />
                    </svg>
                  </span>
                  <span>{`#${item.tag} (${item.count})`}</span>
                </button>
              ))
            )}
          </div>
        </div>

        {tagEditError && <div className="tools-similar-issues">{tagEditError}</div>}

        <div style={{ display: 'flex', gap: 8, marginTop: 16, justifyContent: 'flex-end' }}>
          <button className="btn btn-secondary" onClick={onClose}>
            取消
          </button>
          <button className="btn btn-primary" onClick={onSave}>
            保存
          </button>
        </div>
      </div>
    </div>
  )
}
