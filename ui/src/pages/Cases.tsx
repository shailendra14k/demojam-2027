import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../api'

export default function Cases() {
  const [items, setItems] = useState<any[]>([])
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    api.cases()
      .then((r) => setItems(r.items || []))
      .catch((e) => setErr(String(e)))
  }, [])

  return (
    <section>
      <h2>Cases</h2>
      <p className="muted">Open HIGH / CRITICAL alerts promoted to investigation cases (live data only).</p>
      {err && <p className="error">{err}</p>}
      {!err && items.length === 0 && (
        <p className="muted">No open cases yet — run a demo scenario that produces HIGH/CRITICAL alerts.</p>
      )}
      <table className="table">
        <thead>
          <tr>
            <th>Case</th>
            <th>Player</th>
            <th>Priority</th>
            <th>Status</th>
            <th>Score</th>
            <th>Created</th>
            <th>Network</th>
          </tr>
        </thead>
        <tbody>
          {items.map((c) => (
            <tr key={c.case_id}>
              <td className="mono">{c.case_id}</td>
              <td className="mono">{c.player_id}</td>
              <td>{c.priority}</td>
              <td>{c.status}</td>
              <td>{c.score}</td>
              <td>{c.created_at}</td>
              <td>
                <Link to={`/network/${c.alert_id || c.case_id}`}>Open graph</Link>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  )
}
