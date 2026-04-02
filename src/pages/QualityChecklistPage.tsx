import { useCallback, useEffect, useRef, useState } from 'react'
import './QualityChecklistPage.css'

const CHECKLIST_URL = '/quality-checklist.html'
const MIN_FRAME_HEIGHT = 960

export default function QualityChecklistPage() {
  const frameRef = useRef<HTMLIFrameElement | null>(null)
  const [frameHeight, setFrameHeight] = useState(MIN_FRAME_HEIGHT)

  const syncFrameHeight = useCallback(() => {
    const frame = frameRef.current
    if (!frame) return

    try {
      const doc = frame.contentDocument
      if (!doc) return
      const nextHeight = Math.max(
        doc.body?.scrollHeight || 0,
        doc.documentElement?.scrollHeight || 0,
        MIN_FRAME_HEIGHT,
      )
      setFrameHeight(nextHeight + 16)
    } catch {
      setFrameHeight((current) => Math.max(current, MIN_FRAME_HEIGHT))
    }
  }, [])

  useEffect(() => {
    const intervalId = window.setInterval(syncFrameHeight, 800)
    const timeoutA = window.setTimeout(syncFrameHeight, 180)
    const timeoutB = window.setTimeout(syncFrameHeight, 1200)
    window.addEventListener('resize', syncFrameHeight)

    return () => {
      window.clearInterval(intervalId)
      window.clearTimeout(timeoutA)
      window.clearTimeout(timeoutB)
      window.removeEventListener('resize', syncFrameHeight)
    }
  }, [syncFrameHeight])

  return (
    <section className="quality-checklist-page">
      <div className="quality-checklist-shell">
        <iframe
          ref={frameRef}
          title="YPP 单图歌单过审质检清单"
          src={CHECKLIST_URL}
          className="quality-checklist-frame"
          style={{ height: `${frameHeight}px` }}
          onLoad={() => {
            syncFrameHeight()
            window.setTimeout(syncFrameHeight, 300)
            window.setTimeout(syncFrameHeight, 1500)
          }}
        />
      </div>
    </section>
  )
}
