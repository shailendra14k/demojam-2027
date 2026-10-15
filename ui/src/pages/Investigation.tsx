import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../api'
import { DetectionBadgeFromAlert } from '../components/DetectionBadge'
import { alertDisplayLabel, formatRuleNames, ruleDisplayName } from '../lib/alertLabels'

function when(v: unknown) {
  if (!v) return '—'
  const s = String(v)
  return s.length > 19 ? s.slice(0, 19).replace('T', ' ') : s
}

function jevNeedsRun(alert: any): boolean {
  if (!alert) return false
  if (alert.jev_pending) return true
  const d = alert.jev_decision
  if (!d) return true
  const mode = String(d.mode || '').toLowerCase()
  return mode === '' || mode === 'mock' || mode === 'mock_fallback' || mode === 'pending'
}

export default function Investigation() {
  const [alerts, setAlerts] = useState<any[]>([])
  const [events, setEvents] = useState<any[]>([])
  const [selected, setSelected] = useState<any>(null)
  const [jevBusy, setJevBusy] = useState(false)
  const [jevError, setJevError] = useState<string | null>(null)

  const load = async () => {
    const [a, e] = await Promise.all([api.alerts(), api.events()])
    setAlerts(a.items || [])
    setEvents(e.items || [])
    return a.items || []
  }

  useEffect(() => {
    load().catch(console.error)
    const t = setInterval(() => load().catch(console.error), 8000)
    return () => clearInterval(t)
  }, [])

  const mergeJev = (alertId: string, out: any) => {
    setSelected((s: any) =>
      s?.alert_id === alertId
        ? {
            ...s,
            status: out.status || s.status,
            jev_pending: !!out.pending,
            jev_decision: out.jev_decision,
            jev_disposition: out.jev_decision?.disposition,
            jev_suggested_status: out.jev_decision?.suggested_status,
          }
        : s,
    )
    setAlerts((list) =>
      list.map((a) =>
        a.alert_id === alertId
          ? {
              ...a,
              status: out.status || a.status,
              jev_pending: !!out.pending,
              jev_decision: out.jev_decision,
              jev_disposition: out.jev_decision?.disposition,
              jev_suggested_status: out.jev_decision?.suggested_status,
            }
          : a,
      ),
    )
  }

  const runJev = async (apply: boolean, opts?: { silent?: boolean }) => {
    if (!selected?.alert_id) return
    const alertId = selected.alert_id
    if (!opts?.silent) setJevBusy(true)
    setJevError(null)
    try {
      let out = await api.jevDecide(alertId, apply)
      mergeJev(alertId, out)
      // Poll while OpenJev is still running in the background.
      let tries = 0
      while (out.pending && tries < 40) {
        await new Promise((r) => setTimeout(r, 1500))
        out = await api.jevDecide(alertId, false)
        mergeJev(alertId, out)
        tries += 1
        if (!out.pending && !jevNeedsRun({ jev_decision: out.jev_decision })) break
      }
      await load()
      if (out.pending) {
        setJevError('OpenJev is still running — click Run OpenJev again in a moment.')
      }
    } catch (err: any) {
      setJevError(err?.message || 'OpenJev request failed')
    } finally {
      setJevBusy(false)
    }
  }

  const selectAlert = (a: any) => {
    setSelected(a)
    setJevError(null)
  }

  // Auto-run OpenJev when an alert is selected without a live decision.
  useEffect(() => {
    if (!selected?.alert_id) return
    if (!jevNeedsRun(selected)) return
    if (jevBusy) return
    runJev(false, { silent: true }).catch(() => undefined)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selected?.alert_id])

  const update = async (id: string, status: string) => {
    await api.patchAlert(id, { status })
    await load()
    if (selected?.alert_id === id) {
      setSelected((s: any) => ({ ...s, status }))
    }
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
                      <tr key={`${h.rule_id || h.rule_name}-${i}`}>
                        <td>{ruleDisplayName(h)}</td>
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
                  Detection remains Rules + XGBoost + TFM. OpenJev runs automatically on new
                  alerts and recommends disposition (escalate / need_info / close). Use the
                  button if a decision is still missing.
                </p>
                {(jevBusy || selected.jev_pending || selected.jev_decision?.mode === 'pending') && (
                  <p className="muted">Running OpenJev…</p>
                )}
                {jevError && <p className="error">{jevError}</p>}
                {!selected.jev_decision && !jevBusy && (
                  <p className="muted">No decision yet — starting OpenJev…</p>
                )}
                {selected.jev_decision && selected.jev_decision.mode !== 'pending' && (
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
                  <button
                    className="btn secondary"
                    type="button"
                    disabled={jevBusy}
                    onClick={() => runJev(false)}
                  >
                    {jevBusy ? 'Running OpenJev…' : 'Run OpenJev'}
                  </button>
                  <button
                    className="btn"
                    type="button"
                    disabled={jevBusy || !selected.jev_decision || selected.jev_decision.mode === 'pending'}
                    onClick={() => runJev(true)}
                  >
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
                          <td>{formatRuleNames(e.rules?.hits)}</td>
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
