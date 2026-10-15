import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../api'
import { DetectionBadgeFromAlert } from '../components/DetectionBadge'
import { alertDisplayLabel } from '../lib/alertLabels'

function when(v: unknown) {
  if (!v) return '—'
  const s = String(v)
  return s.length > 19 ? s.slice(0, 19).replace('T', ' ') : s
}

export default function Investigation() {
  const [alerts, setAlerts] = useState<any[]>([])
  const [events, setEvents] = useState<any[]>([])
  const [selected, setSelected] = useState<any>(null)

  const load = async () => {
    const [a, e] = await Promise.all([api.alerts(), api.events()])
    setAlerts(a.items || [])
    setEvents(e.items || [])
  }

  useEffect(() => {
    load().catch(console.error)
    const t = setInterval(() => load().catch(console.error), 8000)
    return () => clearInterval(t)
  }, [])

  const update = async (id: string, status: string) => {
    await api.patchAlert(id, { status })
    await load()
    if (selected?.alert_id === id) {
      setSelected((s: any) => ({ ...s, status }))
    }
  }

  const runJev = async (apply: boolean) => {
    if (!selected?.alert_id) return
    const out = await api.jevDecide(selected.alert_id, apply)
    await load()
    setSelected((s: any) =>
      s?.alert_id === selected.alert_id
        ? {
            ...s,
            status: out.status || s.status,
            jev_decision: out.jev_decision,
            jev_disposition: out.jev_decision?.disposition,
            jev_suggested_status: out.jev_decision?.suggested_status,
          }
        : s,
    )
  }

  const selectAlert = (a: any) => {
    setSelected(a)
  }

  const supporting = useMemo(() => {
    if (!selected) return []
    const pid = String(selected.player_id || '')
    const tid = String(selected.transaction_id || '')
    const cid = String(selected.correlation_id || '')
    return events
      .filter((e) => {
        const ev = e.event || {}
        return (
          String(ev.player_id || '') === pid ||
          String(ev.transaction_id || '') === tid ||
          (cid && String(ev.correlation_id || '') === cid)
        )
      })
      .slice(0, 12)
  }, [selected, events])

  const timeline = useMemo(() => {
    if (!selected) return []
    const items: { when: string; label: string }[] = []
    if (selected.created_at) items.push({ when: selected.created_at, label: 'Alert created' })
    if (selected.updated_at && selected.updated_at !== selected.created_at) {
      items.push({ when: selected.updated_at, label: `Status → ${selected.status}` })
    }
    for (const note of selected.notes || []) {
      items.push({ when: selected.updated_at || selected.created_at, label: `Note: ${note}` })
    }
    for (const e of supporting) {
      items.push({
        when: e.event?.timestamp || '',
        label: `${e.event?.transaction_type || e.event?.event_type} ${e.event?.amount ?? ''} → ${e.risk?.risk_level || '—'}`,
      })
    }
    return items
      .filter((i) => i.when)
      .sort((a, b) => String(a.when).localeCompare(String(b.when)))
  }, [selected, supporting])

  return (
    <div>
      <section className="hero">
        <h2>Investigation desk</h2>
        <p>Review alerts with programmatic explanations — never “AI says high risk”.</p>
      </section>
      <div className="grid">
        <div className="panel span-5">
          <h3>Alerts</h3>
          <table>
            <thead>
              <tr>
                <th>Detection</th>
                <th>Status</th>
                <th>OpenJev</th>
                <th>Score</th>
                <th>Player</th>
              </tr>
            </thead>
            <tbody>
              {alerts.map((a) => (
                <tr
                  key={a.alert_id}
                  role="button"
                  tabIndex={0}
                  onClick={() => selectAlert(a)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                      e.preventDefault()
                      selectAlert(a)
                    }
                  }}
                  style={{
                    cursor: 'pointer',
                    outline:
                      selected?.alert_id === a.alert_id ? '1px solid var(--accent)' : undefined,
                  }}
                >
                  <td>
                    <DetectionBadgeFromAlert alert={a} />
                  </td>
                  <td>{a.status}</td>
                  <td className="mono">
                    {a.jev_disposition || a.jev_decision?.disposition || '—'}
                  </td>
                  <td className="mono">{a.score?.toFixed?.(2)}</td>
                  <td className="mono">
                    <Link to={`/player/${a.player_id}`} onClick={(e) => e.stopPropagation()}>
                      {String(a.player_id).slice(0, 8)}
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="panel span-7">
          <h3>Case detail</h3>
          {!selected && <p className="muted">Select an alert.</p>}
          {selected && (
            <div className="stack">
              <div>
                <strong>{alertDisplayLabel(selected)}</strong>
              </div>
              <div className="mono muted">{selected.alert_id}</div>
              <div>
                Score{' '}
                <span className="mono">{selected.score?.toFixed?.(4) ?? selected.score}</span> ·{' '}
                <span className={`badge ${selected.risk_level}`}>{selected.risk_level}</span> ·{' '}
                {selected.status}
              </div>
              <div>{selected.summary}</div>

              <div>
                <h3>Rules fired</h3>
                {(selected.rule_hits || []).length === 0 && (
                  <p className="muted">No rule hits attached.</p>
                )}
                <table>
                  <thead>
                    <tr>
                      <th>Rule</th>
                      <th>Severity</th>
                      <th>Score</th>
                      <th>Evidence</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(selected.rule_hits || []).map((h: any, i: number) => (
                      <tr key={`${h.rule_id}-${i}`}>
                        <td className="mono">
                          {h.rule_id} {h.rule_name ? `· ${h.rule_name}` : ''}
                        </td>
                        <td>
                          <span className={`badge ${h.severity}`}>{h.severity}</span>
                        </td>
                        <td className="mono">{h.score}</td>
                        <td className="mono">{JSON.stringify(h.evidence || {})}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div>
                <h3>Explanations</h3>
                <ul>
                  {(selected.explanations || []).map((e: string, i: number) => (
                    <li key={i}>{e}</li>
                  ))}
                </ul>
              </div>

              <div>
                <h3>OpenJev decision layer</h3>
                <p className="muted">
                  Detection remains Rules + XGBoost + TFM. OpenJev (open-source System One)
                  only recommends disposition (escalate / need_info / close).
                </p>
                {!selected.jev_decision && (
                  <p className="muted">No decision yet — run Jev on this alert.</p>
                )}
                {selected.jev_decision && (
                  <>
                    <div>
                      Disposition{' '}
                      <span className="badge HIGH">{selected.jev_decision.disposition}</span>
                      {' · '}
                      Jev risk{' '}
                      <span className={`badge ${String(selected.jev_decision.risk_level || '').toUpperCase()}`}>
                        {selected.jev_decision.risk_level}
                      </span>
                      {' · '}
                      <span className="mono muted">
                        conf {(selected.jev_decision.disposition_confidence ?? 0).toFixed?.(2) ?? '—'} ·{' '}
                        {selected.jev_decision.mode}
                      </span>
                    </div>
                    <p>{selected.jev_decision.rationale}</p>
                    <table>
                      <thead>
                        <tr>
                          <th>Red flag</th>
                          <th>Noul</th>
                        </tr>
                      </thead>
                      <tbody>
                        {Object.entries(selected.jev_decision.red_flags || {}).map(([k, v]) => (
                          <tr key={k}>
                            <td className="mono">{k}</td>
                            <td className="mono">{typeof v === 'number' ? v.toFixed(2) : String(v)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    <p className="muted">
                      Suggested status:{' '}
                      <span className="mono">{selected.jev_decision.suggested_status}</span>
                    </p>
                  </>
                )}
                <div className="btn-row" style={{ marginTop: '0.75rem' }}>
                  <button className="btn secondary" type="button" onClick={() => runJev(false)}>
                    Run OpenJev
                  </button>
                  <button className="btn" type="button" onClick={() => runJev(true)}>
                    Apply OpenJev disposition
                  </button>
                </div>
              </div>

              <div>
                <h3>Supporting transactions</h3>
                {supporting.length === 0 && (
                  <p className="muted">
                    No scored events in the live buffer for this player yet. Tx id:{' '}
                    <span className="mono">{selected.transaction_id || '—'}</span>
                  </p>
                )}
                {supporting.length > 0 && (
                  <table>
                    <thead>
                      <tr>
                        <th>When</th>
                        <th>Type</th>
                        <th>Amount</th>
                        <th>Risk</th>
                        <th>Rules</th>
                      </tr>
                    </thead>
                    <tbody>
                      {supporting.map((e, i) => (
                        <tr key={e.event?.event_id || e.event?.transaction_id || i}>
                          <td className="mono">{when(e.event?.timestamp)}</td>
                          <td>{e.event?.transaction_type || e.event?.event_type}</td>
                          <td className="mono">{e.event?.amount}</td>
                          <td>
                            <span className={`badge ${e.risk?.risk_level}`}>
                              {e.risk?.risk_level}
                            </span>
                          </td>
                          <td className="mono">
                            {(e.rules?.hits || []).map((h: any) => h.rule_id).join(', ') || '—'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>

              <div>
                <h3>Timeline</h3>
                {timeline.length === 0 && <p className="muted">No timeline points.</p>}
                <ul>
                  {timeline.map((t, i) => (
                    <li key={i}>
                      <span className="mono">{when(t.when)}</span> — {t.label}
                    </li>
                  ))}
                </ul>
              </div>

              <div className="btn-row">
                <Link className="btn" to={`/network/${selected.alert_id}`}>
                  Open network view
                </Link>
                <button className="btn secondary" onClick={() => update(selected.alert_id, 'INVESTIGATING')}>
                  Investigate
                </button>
                <button
                  className="btn secondary"
                  onClick={() => update(selected.alert_id, 'ESCALATED')}
                >
                  Escalate
                </button>
                <button
                  className="btn secondary"
                  onClick={() => update(selected.alert_id, 'FALSE_POSITIVE')}
                >
                  False positive
                </button>
                <button className="btn secondary" onClick={() => update(selected.alert_id, 'CLOSED')}>
                  Close
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
