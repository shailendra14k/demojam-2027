import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { API_BASE, api } from '../api'

export default function LiveMonitor() {
  const [events, setEvents] = useState<any[]>([])
  const [live, setLive] = useState<string>('connecting')

  useEffect(() => {
    let cancelled = false
    const refresh = () =>
      api
        .events()
        .then((r) => {
          if (!cancelled) setEvents(r.items || [])
        })
        .catch(console.error)

    refresh()
    // Relative /api works behind frontend nginx; VITE_API_URL overrides for local/dev.
    const es = new EventSource(`${API_BASE}/api/v1/sse`)
    es.onopen = () => setLive('live')
    es.onerror = () => setLive('reconnecting')
    es.onmessage = (msg) => {
      try {
        const payload = JSON.parse(msg.data)
        if (payload.type === 'event' || payload.type === 'alert') {
          refresh()
        }
      } catch {
        /* ignore */
      }
    }
    // Polling fallback if SSE is buffered/idle behind a proxy.
    const poll = window.setInterval(refresh, 5000)
    return () => {
      cancelled = true
      es.close()
      window.clearInterval(poll)
    }
  }, [])

  return (
    <div>
      <section className="hero">
        <h2>Live monitor <span className="badge LOW">{live}</span></h2>
        <p>SSE stream of scored transactions as they traverse the AML pipeline.</p>
      </section>
      <div className="panel">
        <table>
          <thead>
            <tr><th>Time</th><th>Player</th><th>Type</th><th>Amount</th><th>Risk</th><th>Score</th><th>Rules</th></tr>
          </thead>
          <tbody>
            {events.length === 0 ? (
              <tr><td colSpan={7} className="mono">Waiting for scored events…</td></tr>
            ) : (
              events.map((e, i) => (
                <tr key={e.event?.event_id || e.event?.transaction_id || i}>
                  <td className="mono">{String(e.event?.timestamp || '').slice(11, 19)}</td>
                  <td className="mono">
                    {e.event?.player_id ? (
                      <Link to={`/player/${e.event.player_id}`}>
                        {String(e.event.player_id).slice(0, 8)}
                      </Link>
                    ) : (
                      '—'
                    )}
                  </td>
                  <td>{e.event?.transaction_type || e.event?.event_type}</td>
                  <td className="mono">{e.event?.amount ?? '—'}</td>
                  <td><span className={`badge ${e.risk?.risk_level}`}>{e.risk?.risk_level}</span></td>
                  <td className="mono">{e.risk?.final_score?.toFixed?.(2) ?? e.risk?.final_score}</td>
                  <td className="mono">{(e.rules?.hits || []).map((h: any) => h.rule_id).join(', ') || '—'}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
