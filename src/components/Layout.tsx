import { useEffect, useState } from 'react'
import { NavLink, Outlet } from 'react-router-dom'
import { api } from '../lib/api'
import type { YoutubeApiUsage } from '../types'

const QUOTA_TIMEZONE = 'America/Los_Angeles'

function getTimeZoneParts(date: Date, timeZone: string) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(date)

  const map: Record<string, string> = {}
  for (const part of parts) {
    if (part.type !== 'literal') map[part.type] = part.value
  }

  return {
    year: Number(map.year),
    month: Number(map.month),
    day: Number(map.day),
    hour: Number(map.hour),
    minute: Number(map.minute),
    second: Number(map.second),
  }
}

function getTimeZoneOffsetMs(date: Date, timeZone: string) {
  const parts = getTimeZoneParts(date, timeZone)
  const zonedUtcMs = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second)
  return zonedUtcMs - date.getTime()
}

function zonedTimeToUtcDate(timeZone: string, year: number, month: number, day: number, hour = 0, minute = 0, second = 0) {
  const utcGuessMs = Date.UTC(year, month - 1, day, hour, minute, second)
  let candidate = new Date(utcGuessMs - getTimeZoneOffsetMs(new Date(utcGuessMs), timeZone))
  const correctedOffset = getTimeZoneOffsetMs(candidate, timeZone)
  candidate = new Date(utcGuessMs - correctedOffset)
  return candidate
}

function getResetCountdown() {
  const now = new Date()
  const zoneNow = getTimeZoneParts(now, QUOTA_TIMEZONE)
  const nextDateRef = new Date(Date.UTC(zoneNow.year, zoneNow.month - 1, zoneNow.day + 1))
  const nextMidnightUtc = zonedTimeToUtcDate(
    QUOTA_TIMEZONE,
    nextDateRef.getUTCFullYear(),
    nextDateRef.getUTCMonth() + 1,
    nextDateRef.getUTCDate(),
    0,
    0,
    0,
  )

  const diffMs = Math.max(0, nextMidnightUtc.getTime() - now.getTime())
  const totalSeconds = Math.floor(diffMs / 1000)
  const hours = String(Math.floor(totalSeconds / 3600)).padStart(2, '0')
  const minutes = String(Math.floor((totalSeconds % 3600) / 60)).padStart(2, '0')
  const seconds = String(totalSeconds % 60).padStart(2, '0')
  return `${hours}:${minutes}:${seconds}`
}

export default function Layout() {
  const [usage, setUsage] = useState<YoutubeApiUsage | null>(null)
  const [syncing, setSyncing] = useState(false)
  const [resetCountdown, setResetCountdown] = useState(() => getResetCountdown())

  useEffect(() => {
    let cancelled = false

    const loadUsage = async () => {
      try {
        const next = await api.getYoutubeApiUsage()
        if (!cancelled) setUsage(next)
      } catch {
        if (!cancelled) setUsage(null)
      }
    }

    void loadUsage()
    const handleSettingsChanged = () => {
      void loadUsage()
    }
    const usageTimer = window.setInterval(() => void loadUsage(), 30000)
    const countdownTimer = window.setInterval(() => setResetCountdown(getResetCountdown()), 1000)
    window.addEventListener('ytpanel-settings-changed', handleSettingsChanged)

    return () => {
      cancelled = true
      window.clearInterval(usageTimer)
      window.clearInterval(countdownTimer)
      window.removeEventListener('ytpanel-settings-changed', handleSettingsChanged)
    }
  }, [])

  const handleRunDailySync = async () => {
    if (syncing) return
    setSyncing(true)
    try {
      await api.runDailySync()
    } finally {
      window.setTimeout(() => setSyncing(false), 1200)
    }
  }

  return (
    <div className="app-layout">
      <nav className="navbar">
        <div className="navbar-inner">
          <NavLink to="/" className="navbar-logo">
            <span className="logo-icon">YT</span>
            YTPanel
          </NavLink>

          <div className="navbar-center-zone">
            <div className="navbar-links">
              <NavLink to="/" end className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
                仪表盘
              </NavLink>
              <NavLink to="/channels" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
                频道管理
              </NavLink>
              <NavLink to="/settings" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
                设置
              </NavLink>
            </div>
          </div>

          <div className="navbar-right-zone">
            <div className="navbar-status-pill">
              {usage ? `API ${usage.used_units}/${usage.daily_limit}` : 'API --/--'}
            </div>
            <div className="navbar-status-pill">
              {`重置 ${resetCountdown}`}
            </div>
            <button type="button" className="btn btn-primary btn-sm navbar-sync-btn" onClick={() => void handleRunDailySync()} disabled={syncing}>
              {syncing ? '同步已排队' : '执行每日同步'}
            </button>
          </div>
        </div>
      </nav>

      <div className="main-container">
        <main className="main-content">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
