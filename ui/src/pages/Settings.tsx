import { FormEvent, useEffect, useState } from 'react'
import { api } from '../api'

export default function Settings() {
  const [w, setW] = useState<any>(null)
  const [intervalSec, setIntervalSec] = useState(30)
  const [enabled, setEnabled] = useState(true)
  const [simNote, setSimNote] = useState('')
  const [saving, setSaving] = useState(false)
  const [status, setStatus] = useState('')

  const load = () => {
    api.weights().then(setW).catch(console.error)
    api
      .simulatorSettings()
      .then((s) => {
        setIntervalSec(Number(s.interval_sec) || 30)
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

  return (
    <section className="ops-console">
      <div className="page-head">
        <div>
          <h2>Settings</h2>
          <p className="muted">Scoring weights and background dummy traffic rate.</p>
        </div>
      </div>

      <section className="panel" style={{ marginBottom: '1.5rem' }}>
        <h3>Background simulator</h3>
        <p className="muted">
          Quiet casino traffic written to Postgres (CDC → pipeline). Demo scenarios are separate and unchanged.
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
