import { useEffect, useMemo, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { API_BASE, api } from '../api'
import { DetectionBadgeFromAlert } from '../components/DetectionBadge'
import { formatRuleNames } from '../lib/alertLabels'
import { buildPlainStory, friendlyMoney, friendlyTxType } from '../lib/casePlainLanguage'

function money(amount: unknown, currency = 'USD') {
  const n = Number(amount)
  if (!Number.isFinite(n)) return '—'
  try {
    return new Intl.NumberFormat('en-US', { style: 'currency', currency: currency || 'USD' }).format(n)
  } catch {
    return `$${n.toFixed(2)}`
  }
}

function when(v: unknown) {
  if (!v) return '—'
  const s = String(v)
  if (s.length >= 19) return s.slice(0, 19).replace('T', ' ')
  return s.slice(11, 19) || s
}

/** Shape used by the Investigation-desk Case Detail panel. */
function alertFromCase(detail: any, row?: any | null) {
  const rawAlert = detail?.raw?.alert || {}
  const risk = detail?.raw?.risk || {}
  return {
    alert_id: detail?.alert_id || rawAlert.alert_id,
    player_id: detail?.player_id || rawAlert.player_id,
    transaction_id: detail?.transaction_id || rawAlert.transaction_id,
    event_id: detail?.event_id || rawAlert.event_id,
    correlation_id: rawAlert.correlation_id || row?.correlation_id,
    risk_level: detail?.risk_level || rawAlert.risk_level || risk.risk_level,
    status: detail?.status || rawAlert.status || 'OPEN',
    title: detail?.title || rawAlert.title,
    display_label: detail?.title || rawAlert.display_label || rawAlert.title,
    summary:
      rawAlert.summary ||
      detail?.summary?.what_happened ||
      (Array.isArray(detail?.explanations) ? detail.explanations[0] : '') ||
      '',
    score: detail?.score ?? rawAlert.score ?? risk.final_score ?? 0,
    rule_hits: detail?.rule_hits || rawAlert.rule_hits || risk.rule_hits || [],
    explanations:
      rawAlert.explanations?.length
        ? rawAlert.explanations
        : risk.explanations?.length
          ? risk.explanations
          : detail?.explanations || [],
    notes: rawAlert.notes || [],
    amount: detail?.amount ?? rawAlert.amount ?? row?.amount,
    transaction_type: detail?.transaction_type || rawAlert.transaction_type || row?.transaction_type,
    currency: detail?.currency || rawAlert.currency || row?.currency || 'USD',
    created_at: detail?.created_at || rawAlert.created_at,
    updated_at: detail?.updated_at || rawAlert.updated_at,
  }
}

/** Instant Case Detail from the Alerts row — no network wait. */
function caseDetailFromSeed(a: any): any {
  const txType = String(a.transaction_type || a.event_type || 'TRANSACTION').toUpperCase()
  const amount = Number(a.amount ?? 0)
  const currency = a.currency || 'USD'
  const playerId = a.player_id || 'UNKNOWN'
  const riskLevel = String(a.risk_level || 'MEDIUM').toUpperCase()
  const score = Number(a.score ?? 0)
  const hits = a.rule_hits || []
  const story = buildPlainStory({
    player_id: playerId,
    transaction_type: txType,
    amount,
    currency,
    location: a.location,
    payment_method: a.payment_method,
    risk_level: riskLevel,
    status: a.status || 'OPEN',
    score,
    title: a.title || a.display_label,
    rule_hits: hits,
    explanations: a.explanations,
  })
  return {
    case_id: a.alert_id,
    alert_id: a.alert_id,
    player_id: playerId,
    transaction_id: a.transaction_id || 'TXN-UNKNOWN',
    event_id: a.event_id || a.transaction_id,
    status: a.status || 'OPEN',
    risk_level: riskLevel,
    score,
    title: story.headline,
    amount,
    transaction_type: txType,
    currency,
    location: a.location || 'Casino floor',
    payment_method: a.payment_method || 'UNKNOWN',
    created_at: a.created_at,
    updated_at: a.updated_at || a.created_at,
    summary: {
      headline: story.headline,
      in_short: story.in_short,
      what_happened: story.what_happened,
      why_it_matters: story.why_flagged,
      recommended_next_step: story.what_to_do,
      plain_explanations: [
        story.in_short,
        story.why_flagged,
        ...story.what_rules_mean.map((r) => r.meaning),
      ],
      rule_meanings: story.what_rules_mean,
    },
    explanations: [
      story.in_short,
      story.why_flagged,
      ...story.what_rules_mean.map((r) => r.meaning),
    ],
    rule_hits: hits,
    story,
    raw: {
      alert: a,
      transaction: {
        transaction_id: a.transaction_id,
        player_id: playerId,
        amount,
        transaction_type: txType,
        currency,
      },
      risk: {
        risk_level: riskLevel,
        final_score: score,
        rule_hits: hits,
        explanations: a.explanations || [],
      },
      live_event: null,
      rule_hits: hits,
      status: a.status || 'OPEN',
    },
  }
}

export default function LiveMonitor() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [alerts, setAlerts] = useState<any[]>([])
  const [events, setEvents] = useState<any[]>([])
  const [live, setLive] = useState<string>('connecting')
  const [caseDetail, setCaseDetail] = useState<any | null>(null)
  const [selected, setSelected] = useState<any | null>(null)
  const [caseLoading, setCaseLoading] = useState(false)
  const [caseErr, setCaseErr] = useState<string | null>(null)
  const [tab, setTab] = useState<'summary' | 'json'>('summary')

  const refresh = async () => {
    const [a, e] = await Promise.all([api.alerts(), api.events()])
    setAlerts(a.items || [])
    setEvents(e.items || [])
  }

  useEffect(() => {
    let cancelled = false
    const tick = () =>
      refresh()
        .then(() => {
          if (!cancelled) setLive('live')
        })
        .catch(() => {
          if (!cancelled) setLive('reconnecting')
        })

    tick()
    const es = new EventSource(`${API_BASE}/api/v1/sse`)
    es.onopen = () => setLive('live')
    es.onerror = () => setLive('reconnecting')
    es.onmessage = (msg) => {
      try {
        const payload = JSON.parse(msg.data)
        if (payload?.data?.cleared) {
          setAlerts([])
          setEvents([])
          setCaseDetail(null)
          setSelected(null)
          setLive('live')
          return
        }
        if (payload.type === 'event' || payload.type === 'alert') {
          tick()
        }
      } catch {
        /* ignore */
      }
    }
    const poll = window.setInterval(tick, 5000)
    return () => {
      cancelled = true
      es.close()
      window.clearInterval(poll)
    }
  }, [])

  /** Join Type/Amount from live events when alert payload is thin. */
  const alertRows = useMemo(() => {
    return (alerts || []).map((a) => {
      const tid = String(a.transaction_id || '')
      const eid = String(a.event_id || '')
      const match = events.find((e) => {
        const ev = e.event || {}
        return (
          (tid && String(ev.transaction_id || '') === tid) ||
          (eid && String(ev.event_id || '') === eid) ||
          (a.alert_id && String(e.alert?.alert_id || '') === String(a.alert_id))
        )
      })
      const ev = match?.event || {}
      return {
        ...a,
        amount: a.amount ?? ev.amount ?? 0,
        transaction_type: a.transaction_type || ev.transaction_type || ev.event_type || 'TRANSACTION',
        currency: a.currency || ev.currency || 'USD',
        created_at: a.created_at || ev.timestamp,
      }
    })
  }, [alerts, events])

  const loadCase = async (key: string, opts?: { markInvestigating?: boolean; seed?: any }) => {
    if (!key) return
    setTab('summary')
    setCaseErr(null)
    const seed = opts?.seed || alertRows.find((a) => a.alert_id === key) || null

    // Paint Case Detail immediately from the clicked row (no spinner wait).
    if (seed) {
      const instant = caseDetailFromSeed({
        ...seed,
        status: opts?.markInvestigating ? 'INVESTIGATING' : seed.status,
      })
      setCaseDetail(instant)
      setSelected(alertFromCase(instant, seed))
      setCaseLoading(false)
    } else {
      setCaseLoading(true)
    }

    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev)
        next.set('case', key)
        return next
      },
      { replace: true },
    )

    // Background: enrich from API + mark investigating (do not block UI).
    try {
      const detailPromise = api.caseDetail(key)
      const patchPromise =
        opts?.markInvestigating && key && !String(key).startsWith('CASE-')
          ? api.patchAlert(key, { status: 'INVESTIGATING' }).catch(() => null)
          : Promise.resolve(null)

      const [detail] = await Promise.all([detailPromise, patchPromise])
      setCaseDetail(detail)
      setSelected(
        alertFromCase(detail, {
          ...seed,
          status: opts?.markInvestigating ? 'INVESTIGATING' : seed?.status,
        }),
      )
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev)
          next.set('case', String(detail.alert_id || key))
          return next
        },
        { replace: true },
      )
      // Soft refresh alert list status without blocking Case Detail
      if (opts?.markInvestigating) {
        refresh().catch(() => undefined)
      }
    } catch (e: any) {
      if (!seed) {
        setCaseErr(String(e?.message || e))
        setCaseDetail(null)
        setSelected(null)
      }
    } finally {
      setCaseLoading(false)
    }
  }

  useEffect(() => {
    const q = searchParams.get('case')
    if (!q) return
    if (selected?.alert_id === q) return
    loadCase(q).catch(console.error)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams])

  const selectAlert = (a: any) => {
    const key = String(a.alert_id || a.event_id || a.transaction_id || '')
    if (!key) return
    // Row click opens Case Detail and marks INVESTIGATING
    loadCase(key, { markInvestigating: true, seed: a }).catch(console.error)
  }

  const update = async (status: string) => {
    const id = selected?.alert_id || caseDetail?.alert_id
    if (!id || String(id).startsWith('CASE-')) return
    await api.patchAlert(id, { status })
    await refresh()
    await loadCase(id)
  }

  const closeCase = () => {
    setCaseDetail(null)
    setSelected(null)
    setCaseErr(null)
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev)
        next.delete('case')
        return next
      },
      { replace: true },
    )
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

  const prettyJson = useMemo(() => {
    if (!caseDetail?.raw) return '{}'
    return JSON.stringify(caseDetail.raw, null, 2)
  }, [caseDetail])

  const story = useMemo(() => {
    if (!selected && !caseDetail) return null
    if (caseDetail?.story) return caseDetail.story
    return buildPlainStory({
      player_id: caseDetail?.player_id || selected?.player_id,
      transaction_type: caseDetail?.transaction_type || selected?.transaction_type,
      amount: caseDetail?.amount ?? selected?.amount,
      currency: caseDetail?.currency || selected?.currency,
      location: caseDetail?.location,
      payment_method: caseDetail?.payment_method,
      risk_level: caseDetail?.risk_level || selected?.risk_level,
      status: caseDetail?.status || selected?.status,
      score: caseDetail?.score ?? selected?.score,
      title: caseDetail?.title || caseDetail?.summary?.headline || selected?.title,
      rule_hits: caseDetail?.rule_hits || selected?.rule_hits,
      explanations: caseDetail?.explanations || selected?.explanations,
    })
  }, [caseDetail, selected])

  return (
    <div className="live-monitor-page">
      <section className="hero">
        <h2>
          Live patterns <span className="badge LOW">{live}</span>
        </h2>
        <p>
          Realtime TxnIQ flags <strong>patterns</strong> as transactions stream in — click a row for
          a plain-language case.
        </p>
      </section>

      <div className="grid">
        <div className="panel span-5">
          <h3>Alerts</h3>
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Player</th>
                <th>Type</th>
                <th>Amount</th>
                <th>Rules</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {alertRows.length === 0 ? (
                <tr>
                  <td colSpan={6} className="mono">
                    Waiting for alerts…
                  </td>
                </tr>
              ) : (
                alertRows.map((a) => {
                  const active = selected?.alert_id === a.alert_id
                  return (
                    <tr
                      key={a.alert_id}
                      role="button"
                      tabIndex={0}
                      className={active ? 'live-row-active' : undefined}
                      onClick={() => selectAlert(a)}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                          e.preventDefault()
                          selectAlert(a)
                        }
                      }}
                      style={{ cursor: 'pointer' }}
                    >
                      <td className="mono">{when(a.created_at)}</td>
                      <td className="mono">
                        {a.player_id ? (
                          <Link to={`/player/${a.player_id}`} onClick={(ev) => ev.stopPropagation()}>
                            {String(a.player_id).slice(0, 8)}
                          </Link>
                        ) : (
                          '—'
                        )}
                      </td>
                      <td>{a.transaction_type || '—'}</td>
                      <td className="mono">{money(a.amount, a.currency)}</td>
                      <td>{formatRuleNames(a.rule_hits || a.matched_rules)}</td>
                      <td>
                        <span
                          className={`badge ${
                            a.status === 'OPEN' || a.status === 'INVESTIGATING' ? 'HIGH' : 'LOW'
                          }`}
                        >
                          {a.status}
                        </span>
                      </td>
                    </tr>
                  )
                })
              )}
            </tbody>
          </table>
        </div>

        <div className="panel span-7 case-detail-panel">
          <div className="panel-head case-detail-head">
            <div>
              <h3>Case detail</h3>
              {!selected && !caseLoading && <p className="muted">Select an alert row to investigate.</p>}
            </div>
            {selected && (
              <div className="case-tabs">
                <button
                  type="button"
                  className={tab === 'summary' ? 'btn' : 'btn secondary'}
                  onClick={() => setTab('summary')}
                >
                  Summary
                </button>
                <button
                  type="button"
                  className={tab === 'json' ? 'btn' : 'btn secondary'}
                  onClick={() => setTab('json')}
                >
                  JSON data
                </button>
                <button type="button" className="btn secondary" onClick={closeCase}>
                  Close
                </button>
              </div>
            )}
          </div>

          {caseLoading && <p className="muted">Loading case…</p>}
          {caseErr && <p className="muted">Could not load case: {caseErr}</p>}

          {!caseLoading && selected && story && tab === 'summary' && (
            <div className="stack case-plain">
              <div>
                <DetectionBadgeFromAlert alert={selected} />
              </div>
              <p className="case-in-short">{story.in_short}</p>
              <div className="case-meta-plain">
                <span className={`badge ${selected.risk_level}`}>{story.risk_line}</span>
                <span className="badge LOW">{story.status_line}</span>
              </div>

              <div>
                <h3>What happened</h3>
                <p>{caseDetail?.summary?.what_happened || story.what_happened}</p>
              </div>

              <div>
                <h3>Why we flagged this</h3>
                <p>{caseDetail?.summary?.why_it_matters || story.why_flagged}</p>
              </div>

              <div>
                <h3>What each check means</h3>
                {(caseDetail?.summary?.rule_meanings || story.what_rules_mean || []).length === 0 ? (
                  <p className="muted">No specific rule names were attached — the overall pattern still looked unusual.</p>
                ) : (
                  <ul className="case-explain-list">
                    {(caseDetail?.summary?.rule_meanings || story.what_rules_mean).map(
                      (r: { name: string; meaning: string }, i: number) => (
                        <li key={`${r.name}-${i}`}>
                          <strong>{r.name}</strong> — {r.meaning}
                        </li>
                      ),
                    )}
                  </ul>
                )}
              </div>

              <div>
                <h3>What you should do</h3>
                <p>{caseDetail?.summary?.recommended_next_step || story.what_to_do}</p>
              </div>

              {supporting.length > 0 && (
                <div>
                  <h3>Other recent activity for this player</h3>
                  <ul className="case-explain-list">
                    {supporting.slice(0, 6).map((e, i) => (
                      <li key={e.event?.event_id || e.event?.transaction_id || i}>
                        {when(e.event?.timestamp)} — {friendlyTxType(e.event?.transaction_type || e.event?.event_type)}{' '}
                        of {friendlyMoney(e.event?.amount, e.event?.currency)}
                        {e.risk?.risk_level ? ` (${String(e.risk.risk_level).toLowerCase()} risk)` : ''}
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              <div className="btn-row">
                <Link className="btn" to={`/network/${selected.alert_id}`}>
                  See linked accounts
                </Link>
                <button type="button" className="btn secondary" onClick={() => update('ESCALATED')}>
                  Send to senior investigator
                </button>
                <button
                  type="button"
                  className="btn secondary"
                  onClick={() => update('FALSE_POSITIVE')}
                >
                  Mark as false alarm
                </button>
                <button type="button" className="btn secondary" onClick={() => update('CLOSED')}>
                  Close case
                </button>
              </div>
            </div>
          )}

          {!caseLoading && caseDetail && tab === 'json' && (
            <pre className="case-json mono">{prettyJson}</pre>
          )}
        </div>
      </div>
    </div>
  )
}
