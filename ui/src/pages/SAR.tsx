import { useEffect, useState } from 'react'
import { api } from '../api'

export default function SAR() {
  const [items, setItems] = useState<any[]>([])

  useEffect(() => {
    api.sars().then((r) => setItems(r.items || [])).catch(console.error)
  }, [])

  return (
    <section>
      <h2>SAR drafts</h2>
      <p className="muted">Suspicious Activity Report drafts from CRITICAL alerts. Empty until critical alerts exist.</p>
      {items.length === 0 && <p className="muted">No SAR drafts yet.</p>}
      <table className="table">
        <thead>
          <tr>
            <th>SAR</th>
            <th>Player</th>
            <th>Status</th>
            <th>Narrative</th>
          </tr>
        </thead>
        <tbody>
          {items.map((s) => (
            <tr key={s.sar_id}>
              <td className="mono">{s.sar_id}</td>
              <td className="mono">{s.player_id}</td>
              <td>{s.status}</td>
              <td>{s.narrative}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  )
}
