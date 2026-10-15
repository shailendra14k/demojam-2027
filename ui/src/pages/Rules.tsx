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
      <p className="muted">Catalog of AML detection rules. Thresholds are env-configurable.</p>
      <table className="table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Severity</th>
          </tr>
        </thead>
        <tbody>
          {items.map((r) => (
            <tr key={r.id || r.name}>
              <td>{r.name || r.id}</td>
              <td>{r.severity}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  )
}
