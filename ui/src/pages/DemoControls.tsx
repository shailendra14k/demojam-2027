import { useEffect, useState } from 'react'
import { api } from '../api'

const LABELS: Record<string, string> = {
  normal: 'Generate Normal Transaction',
  structuring: 'Trigger Structuring',
  minimal_gaming: 'Trigger Minimal Gaming',
  rapid_withdrawal: 'Trigger Rapid Withdrawal',
  shared_device: 'Trigger Shared Device',
  behavioral_deviation: 'Trigger Behavioral Deviation',
  rapid_wire: 'Trigger Rapid Wire',
  tito: 'Trigger Large TITO',
  high_velocity: 'Trigger High Velocity',
  watchlist: 'Trigger Watchlist + CTR',
}

const ORDER = [
  'normal',
  'structuring',
  'minimal_gaming',
  'rapid_withdrawal',
  'shared_device',
  'behavioral_deviation',
  'rapid_wire',
  'tito',
  'high_velocity',
  'watchlist',
]

async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms))
}

export default function DemoControls() {
  const [scenarios, setScenarios] = useState<Record<string, any>>({})
  const [result, setResult] = useState<any>(null)
  const [busy, setBusy] = useState<string | null>(null)
  const [status, setStatus] = useState<string | null>(null)

  useEffect(() => {
    api.scenarios().then(setScenarios).catch(console.error)
  }, [])

  const ordered = [
    ...ORDER.filter((n) => scenarios[n]),
    ...Object.keys(scenarios).filter((n) => !ORDER.includes(n)),
  ]

  const run = async (name: string) => {
    setBusy(name)
    setStatus('Injecting into Postgres (CDC)…')
    setResult(null)
    try {
      const r = await api.runScenario(name)
      const expectsAlert = (r.expected?.expected_rules || []).length > 0
      let alerts = r.alerts || []
      let sample = r.sample_risk

      if (expectsAlert && alerts.length === 0) {
        setStatus('Waiting for CDC → Kafka → alert pipeline…')
        const playerIds = new Set((r.player_ids || []).map(String))
        const deadline = Date.now() + 90_000
        while (Date.now() < deadline) {
          await sleep(2500)
          const live = await api.alerts()
          const items = live.items || []
          const matched = playerIds.size
            ? items.filter((a: any) => playerIds.has(String(a.player_id)))
            : items.slice(0, 5)
          // Prefer freshly created alerts (last 3 minutes).
          const fresh = matched.filter((a: any) => {
            const t = Date.parse(a.created_at || '')
            return Number.isFinite(t) && Date.now() - t < 180_000
          })
          const hit = fresh.length ? fresh : matched
          if (hit.length) {
            alerts = hit
            sample = {
              risk_level: hit[0].risk_level,
              final_score: hit[0].score,
            }
            break
          }
          setStatus(`Still waiting for alerts… ${Math.max(0, Math.round((deadline - Date.now()) / 1000))}s`)
        }
      } else if (!expectsAlert) {
        // Give Live Monitor a moment; benign txs still score via CDC.
        setStatus('Normal activity injected — check Live Monitor for the scored row.')
        await sleep(1500)
      }

      const final = {
        ...r,
        alerts,
        alerts_generated: alerts.length,
        sample_risk: sample || r.sample_risk,
      }
      setResult(final)
      setStatus(
        alerts.length
          ? `Done — ${alerts.length} alert(s). Open Live Monitor / Investigation.`
          : expectsAlert
            ? 'Injected, but no alert yet — pipeline may still be catching up; refresh Investigation shortly.'
            : 'Done — benign transaction injected.',
      )
    } catch (e: any) {
      setStatus(`Failed: ${e?.message || e}`)
      setResult({ error: String(e?.message || e), scenario: name })
    } finally {
      setBusy(null)
    }
  }

  return (
    <div>
      <section className="hero">
        <h2>Demo controls</h2>
        <p>Inject deterministic AML scenarios into the live scoring pipeline. Suspicious runs surface alerts without a manual page refresh once CDC drains.</p>
      </section>
      <div className="grid">
        {ordered.map((name) => {
          const meta = scenarios[name] || {}
          return (
            <div className="panel span-6" key={name}>
              <h3>{LABELS[name] || name}</h3>
              <p className="muted">{meta.description}</p>
              <p className="mono">
                Expected rules:{' '}
                {(meta.expected_rules || meta.rule_ids || []).join(', ') || 'none (benign)'}
              </p>
              <button className="btn" disabled={!!busy} onClick={() => run(name)}>
                {busy === name ? 'Running…' : LABELS[name] || 'Run scenario'}
              </button>
            </div>
          )
        })}
      </div>
      {status && (
        <p className="muted" style={{ marginTop: '1rem' }}>
          {status}
        </p>
      )}
      {result && (
        <div className="panel" style={{ marginTop: '1rem' }}>
          <h3>Last run: {result.scenario || busy}</h3>
          {result.error ? (
            <p style={{ color: 'var(--danger)' }}>{result.error}</p>
          ) : (
            <>
              <p>
                Seed {result.seed} · Events: {result.events_injected ?? result.written?.length ?? '—'} ·
                Alerts: {result.alerts_generated ?? (result.alerts || []).length}
              </p>
              <p>
                Sample risk:{' '}
                <span className={`badge ${result.sample_risk?.risk_level}`}>
                  {result.sample_risk?.risk_level || '—'}
                </span>{' '}
                score {result.sample_risk?.final_score ?? '—'}
              </p>
              <pre
                className="mono"
                style={{ whiteSpace: 'pre-wrap', maxHeight: 240, overflow: 'auto' }}
              >
                {JSON.stringify(result.expected || result, null, 2)}
              </pre>
            </>
          )}
        </div>
      )}
    </div>
  )
}
