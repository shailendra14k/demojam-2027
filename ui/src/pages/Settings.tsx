import { FormEvent, useEffect, useState } from 'react'
import { api } from '../api'

export default function Settings() {
  const [w, setW] = useState<any>(null)
  const [intervalSec, setIntervalSec] = useState(60)
  const [enabled, setEnabled] = useState(true)
  const [simNote, setSimNote] = useState('')
  const [saving, setSaving] = useState(false)
  const [clearing, setClearing] = useState(false)
  const [status, setStatus] = useState('')

  const load = () => {
    api.weights().then(setW).catch(console.error)
    api
      .simulatorSettings()
      .then((s) => {
        setIntervalSec(Number(s.interval_sec) || 60)
        setEnabled(Boolean(s.enabled))
        setSimNote(s.note || '')
      })
      .catch((e) => setStatus(String(e)))
  }

  useEffect(() => {
    load()
  }, [])

  const onSave = async (e: FormEvent) => {
    e.preventDefault()
    setSaving(true)
    setStatus('')
    try {
      const s = await api.patchSimulatorSettings({
        interval_sec: Number(intervalSec),
        enabled,
      })
      setIntervalSec(Number(s.interval_sec) || intervalSec)
      setEnabled(Boolean(s.enabled))
      setSimNote(s.note || '')
      setStatus(`Saved: 1 transaction every ${s.interval_sec}s (${s.enabled ? 'on' : 'paused'})`)
    } catch (err) {
      setStatus(String(err))
    } finally {
      setSaving(false)
    }
  }

  const onClearLive = async () => {
    if (
      !window.confirm(
        'Clear all live monitor transactions and alerts, then seed fresh demo traffic with complete fields?',
      )
    ) {
      return
    }
    setClearing(true)
    setStatus('')
    try {
      const r = await api.clearLive({
        seed: true,
        scenarios: ['structuring', 'rapid_wire', 'shared_device'],
      })
      const seeded = (r.seeded || []).map((s: any) => s.scenario).join(', ') || 'none'
      setStatus(
        `Cleared ${r.cleared_live_events ?? 0} live rows and ${r.cleared_memory_alerts ?? 0} alerts. ` +
          `DB removed alerts=${r.db_deleted?.alerts ?? 0}, txns=${r.db_deleted?.transactions ?? 0}. ` +
          `Seeded: ${seeded}. New scores appear as CDC finishes.`,
      )
    } catch (err) {
      setStatus(String(err))
    } finally {
      setClearing(false)
    }
  }

  return (
    <section className="ops-console">
      <div className="page-head">
        <div>
          <h2>Settings</h2>
          <p className="muted">Scoring weights, live traffic rate, and live data reset.</p>
        </div>
      </div>

      <section className="panel" style={{ marginBottom: '1.5rem' }}>
        <h3>Background simulator</h3>
        <p className="muted">
          Quiet casino traffic written to Postgres (CDC → pipeline). Default is one transaction every
          60 seconds (1 per minute). Change the interval anytime.
        </p>
        <form onSubmit={onSave} className="settings-form">
          <label>
            Interval (seconds)
            <input
              type="number"
              min={1}
              max={3600}
              step={1}
              value={intervalSec}
              onChange={(e) => setIntervalSec(Number(e.target.value))}
            />
          </label>
          <label className="settings-check">
            <input type="checkbox" checked={enabled} onChange={(e) => setEnabled(e.target.checked)} />
            Enabled (uncheck to pause dummy traffic)
          </label>
          <button type="submit" disabled={saving}>
            {saving ? 'Saving…' : 'Save simulator settings'}
          </button>
        </form>
        {simNote && <p className="muted">{simNote}</p>}
        {status && <p className="muted">{status}</p>}
      </section>

      <section className="panel" style={{ marginBottom: '1.5rem' }}>
        <h3>Live monitor data</h3>
        <p className="muted">
          Remove every live transaction and alert from the stream, wipe demo activity rows, then
          insert fresh scenarios with Type, Amount, Status, and location filled in.
        </p>
        <button type="button" className="btn" disabled={clearing} onClick={onClearLive}>
          {clearing ? 'Clearing…' : 'Clear all live transactions'}
        </button>
      </section>

      <section className="panel">
        <h3>Scoring weights</h3>
        <p className="muted">Live ConfigMap / environment values (not hardcoded demo copy).</p>
        {!w && <p className="muted">Loading…</p>}
        {w && (
          <ul>
            <li>
              Rules: <strong>{w.rules}</strong>
            </li>
            <li>
              ML (XGBoost): <strong>{w.ml}</strong>
            </li>
            <li>
              Behavioral: <strong>{w.behavioral}</strong>
            </li>
            <li>
              KYC (optional): <strong>{w.kyc}</strong>
            </li>
            <li>
              Alert threshold: <strong>{w.alert_threshold}</strong>
            </li>
          </ul>
        )}
        {w?.note && <p className="muted">{w.note}</p>}
      </section>
    </section>
  )
}
