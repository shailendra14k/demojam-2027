import { useEffect, useState } from 'react'
import { api } from '../api'

export default function Rules() {
  const [items, setItems] = useState<any[]>([])

  useEffect(() => {
    api.rulesCatalog().then((r) => setItems(r.items || [])).catch(console.error)
  }, [])

  return (
    <section>
      <h2>Rules catalog</h2>
      <p className="muted">AML-001…015 — ours plus cage/BSA rules from demojam. Thresholds are env-configurable.</p>
      <table className="table">
        <thead>
          <tr>
            <th>ID</th>
            <th>Name</th>
            <th>Severity</th>
          </tr>
        </thead>
        <tbody>
          {items.map((r) => (
            <tr key={r.id}>
              <td className="mono">{r.id}</td>
              <td>{r.name}</td>
              <td>{r.severity}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  )
}
