import type { ReportingRequestLog } from '../../types'

type Props = {
  logs: ReportingRequestLog[]
}

export default function OwnerRequestLogPanel({ logs }: Props) {
  return (
    <div className="settings-form" style={{ gap: 14 }}>
      <div className="panel-header">
        <div>
          <h3>请求日志</h3>
          <p>按 Owner 维度查看最近请求、状态码与错误信息。</p>
        </div>
      </div>

      {logs.length === 0 ? (
        <div className="empty-state">暂无请求日志。</div>
      ) : (
        <div style={{ overflowX: 'auto' }}>
          <table className="table">
            <thead>
              <tr>
                <th>时间</th>
                <th>类型</th>
                <th>状态</th>
                <th>耗时</th>
                <th>错误</th>
              </tr>
            </thead>
            <tbody>
              {logs.map((log) => (
                <tr key={log.id}>
                  <td>{log.started_at}</td>
                  <td>{log.request_kind}</td>
                  <td>{log.status_code ?? '-'}</td>
                  <td>{log.duration_ms ?? '-'}</td>
                  <td>{log.error_message || '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
