import { useEffect, useState } from 'react'
import { api } from '../api'

type RuleItem = {
  id: string
  name: string
  severity?: string
  pattern?: string
  description?: string
  plain_english?: string
  category?: string
  category_label?: string
}

export default function Rules() {
  const [items, setItems] = useState<RuleItem[]>([])
  const [meta, setMeta] = useState<any>(null)
  const [selected, setSelected] = useState<RuleItem | null>(null)
  const [err, setErr] = useState<string | null>(null)

  const load = async () => {
    setErr(null)
    try {
      const [ind, catalog] = await Promise.all([api.industrySettings(), api.rulesCatalog()])
      setMeta({
        ...catalog,
        category: catalog.category || ind.category,
        category_label: catalog.category_label || ind.label,
      })
      setItems(catalog.items || [])
      setSelected((prev) => {
        if (!prev) return null
        return (catalog.items || []).find((r: RuleItem) => r.id === prev.id) || null
      })
    } catch (e: any) {
      setErr(String(e?.message || e))
    }
  }

  useEffect(() => {
    load().catch(console.error)
  }, [])

  return (
    <section className="ops-console">
      <div className="page-head">
        <div>
          <h2>Pattern rules</h2>
          <p className="muted">
            Pre-populated for the selected industry. Realtime TxnIQ watches for{' '}
            <strong>patterns over time</strong>, not a single moment-in-time event.
          </p>
        </div>
      </div>

      {meta && (
        <p className="muted" style={{ marginBottom: '1rem' }}>
          Category: <strong>{meta.category_label || meta.category}</strong>
          {meta.pattern_focus ? ` · Focus: ${meta.pattern_focus}` : ''}
        </p>
      )}
      {err && <p className="muted">Could not load rules: {err}</p>}

      <div className="grid">
        <div className="panel span-5">
          <h3>Rules for this category</h3>
          <table className="table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Severity</th>
                <th>Pattern</th>
              </tr>
            </thead>
            <tbody>
              {items.length === 0 ? (
                <tr>
                  <td colSpan={3} className="muted">
                    No rules loaded. Choose an industry in Settings.
                  </td>
                </tr>
              ) : (
                items.map((r) => (
                  <tr
                    key={r.id}
                    role="button"
                    tabIndex={0}
                    className={selected?.id === r.id ? 'live-row-active' : undefined}
                    style={{ cursor: 'pointer' }}
                    onClick={() => setSelected(r)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault()
                        setSelected(r)
                      }
                    }}
                  >
                    <td>{r.name}</td>
                    <td>
                      <span className={`badge ${r.severity || 'MEDIUM'}`}>{r.severity}</span>
                    </td>
                    <td className="mono muted">{String(r.pattern || '').replace(/_/g, ' ')}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        <div className="panel span-7">
          <h3>What this rule does</h3>
          {!selected && <p className="muted">Click a rule to read a plain-English explanation.</p>}
          {selected && (
            <div className="stack case-plain">
              <div>
                <strong style={{ fontSize: '1.05rem' }}>{selected.name}</strong>
                <div className="muted" style={{ marginTop: '0.35rem' }}>
                  {selected.id} · {selected.severity || 'MEDIUM'}
                  {selected.pattern ? ` · ${String(selected.pattern).replace(/_/g, ' ')}` : ''}
                </div>
              </div>
              <div>
                <h3>In plain English</h3>
                <p>{selected.plain_english || selected.description || 'No description yet.'}</p>
              </div>
              <div>
                <h3>Why it is a pattern rule</h3>
                <p>
                  This check looks at behavior across multiple events or linked entities — not just
                  one transaction in isolation. That is how Realtime TxnIQ spots risk as money moves.
                </p>
              </div>
            </div>
          )}
        </div>
      </div>
    </section>
  )
}
