import { FormEvent, useEffect, useState } from 'react'
import { api } from '../api'

type Industry = {
  id: string
  label: string
  tagline?: string
  pattern_focus?: string
  rule_count?: number
}

export default function Settings() {
  const [industries, setIndustries] = useState<Industry[]>([])
  const [category, setCategory] = useState('casino')
  const [industryLabel, setIndustryLabel] = useState('')
  const [intervalSec, setIntervalSec] = useState(60)
  const [enabled, setEnabled] = useState(true)
  const [simNote, setSimNote] = useState('')
  const [rulesPct, setRulesPct] = useState(45)
  const [mlPct, setMlPct] = useState(35)
  const [behavioralPct, setBehavioralPct] = useState(20)
  const [kycPct, setKycPct] = useState(10)
  const [thresholdPct, setThresholdPct] = useState(55)
  const [saving, setSaving] = useState(false)
  const [savingWeights, setSavingWeights] = useState(false)
  const [savingIndustry, setSavingIndustry] = useState(false)
  const [clearing, setClearing] = useState(false)
  const [status, setStatus] = useState('')

  const load = () => {
    api
      .industrySettings()
      .then((s) => {
        setCategory(s.category || 'casino')
        setIndustryLabel(s.label || '')
        setIndustries(s.industries || [])
      })
      .catch((e) => setStatus(String(e)))
    api
      .weights()
      .then((w) => {
        setRulesPct(Number(w.rules_pct ?? w.rules * 100) || 45)
        setMlPct(Number(w.ml_pct ?? w.ml * 100) || 35)
        setBehavioralPct(Number(w.behavioral_pct ?? w.behavioral * 100) || 20)
        setKycPct(Number(w.kyc_pct ?? w.kyc * 100) || 10)
        setThresholdPct(Number(w.alert_threshold_pct ?? w.alert_threshold * 100) || 55)
      })
      .catch(console.error)
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

  const weightSum = rulesPct + mlPct + behavioralPct + kycPct

  const onSaveIndustry = async (e: FormEvent) => {
    e.preventDefault()
    setSavingIndustry(true)
    setStatus('')
    try {
      const s = await api.patchIndustry(category)
      setCategory(s.category)
      setIndustryLabel(s.label || '')
      setIndustries(s.industries || industries)
      setStatus(`Industry set to ${s.label || s.category}. Open Rules to see that pack.`)
    } catch (err) {
      setStatus(String(err))
    } finally {
      setSavingIndustry(false)
    }
  }

  const onSaveWeights = async (e: FormEvent) => {
    e.preventDefault()
    setSavingWeights(true)
    setStatus('')
    try {
      const w = await api.patchWeights({
        rules_pct: Number(rulesPct),
        ml_pct: Number(mlPct),
        behavioral_pct: Number(behavioralPct),
        kyc_pct: Number(kycPct),
        alert_threshold_pct: Number(thresholdPct),
      })
      setRulesPct(Number(w.rules_pct))
      setMlPct(Number(w.ml_pct))
      setBehavioralPct(Number(w.behavioral_pct))
      setKycPct(Number(w.kyc_pct))
      setThresholdPct(Number(w.alert_threshold_pct))
      setStatus(
        `Score mix saved (rules ${w.rules_pct}% · ML ${w.ml_pct}% · behavioral ${w.behavioral_pct}% · KYC ${w.kyc_pct}%). ` +
          `Alert when score ≥ ${w.alert_threshold_pct}%.`,
      )
    } catch (err) {
      setStatus(String(err))
    } finally {
      setSavingWeights(false)
    }
  }

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
        'Clear Live Monitor completely? This removes all live transactions and alerts and pauses background traffic. Demo buttons will inject one scenario at a time.',
      )
    ) {
      return
    }
    setClearing(true)
    setStatus('')
    try {
      const r = await api.clearLive({
        seed: false,
        pause_simulator: true,
      })
      setEnabled(false)
      setStatus(
        `Live is empty. Cleared ${r.cleared_live_events ?? 0} live rows and ${r.cleared_memory_alerts ?? 0} alerts. ` +
          `DB removed alerts=${r.db_deleted?.alerts ?? 0}, txns=${r.db_deleted?.transactions ?? 0}. ` +
          `Simulator paused — use Demo to inject a single scenario, or re-enable traffic below.`,
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
          <p className="muted">
            Admin controls for Realtime TxnIQ — industry pack, score mix, and live traffic.
          </p>
        </div>
      </div>

      <section className="panel" style={{ marginBottom: '1.5rem' }}>
        <h3>Industry category</h3>
        <p className="muted">
          Realtime TxnIQ is industry-agnostic. Pick a use-case pack to load pre-populated pattern
          rules. Casino is one available category (and the live demo pack).
        </p>
        <form onSubmit={onSaveIndustry} className="settings-form">
          <label>
            Category
            <select value={category} onChange={(e) => setCategory(e.target.value)}>
              {industries.map((i) => (
                <option key={i.id} value={i.id}>
                  {i.label} ({i.rule_count ?? 0} rules)
                </option>
              ))}
            </select>
          </label>
          <button type="submit" disabled={savingIndustry}>
            {savingIndustry ? 'Saving…' : 'Save industry'}
          </button>
        </form>
        {industryLabel && (
          <p className="muted">
            Active: <strong>{industryLabel}</strong>
            {industries.find((i) => i.id === category)?.tagline
              ? ` — ${industries.find((i) => i.id === category)?.tagline}`
              : ''}
          </p>
        )}
      </section>

      <section className="panel" style={{ marginBottom: '1.5rem' }}>
        <h3>Score mix (%)</h3>
        <p className="muted">
          Configure how much each signal contributes to the final score, and the alert threshold
          percent. Prefer values that sum to 100% (risk engine still normalizes if they do not).
        </p>
        <form onSubmit={onSaveWeights} className="settings-form settings-weights">
          <label>
            Rules %
            <input
              type="number"
              min={0}
              max={100}
              step={1}
              value={rulesPct}
              onChange={(e) => setRulesPct(Number(e.target.value))}
            />
          </label>
          <label>
            ML %
            <input
              type="number"
              min={0}
              max={100}
              step={1}
              value={mlPct}
              onChange={(e) => setMlPct(Number(e.target.value))}
            />
          </label>
          <label>
            Behavioral %
            <input
              type="number"
              min={0}
              max={100}
              step={1}
              value={behavioralPct}
              onChange={(e) => setBehavioralPct(Number(e.target.value))}
            />
          </label>
          <label>
            KYC %
            <input
              type="number"
              min={0}
              max={100}
              step={1}
              value={kycPct}
              onChange={(e) => setKycPct(Number(e.target.value))}
            />
          </label>
          <label>
            Alert threshold %
            <input
              type="number"
              min={0}
              max={100}
              step={1}
              value={thresholdPct}
              onChange={(e) => setThresholdPct(Number(e.target.value))}
            />
          </label>
          <button type="submit" disabled={savingWeights || weightSum <= 0}>
            {savingWeights ? 'Saving…' : 'Save score mix'}
          </button>
        </form>
        <p className="muted">
          Current sum: <strong>{weightSum}%</strong>
          {weightSum === 100
            ? ' (ready)'
            : weightSum > 0
              ? ' (tip: aim for 100%; engine will scale if needed)'
              : ' (enter at least one weight)'}
        </p>
      </section>

      <section className="panel" style={{ marginBottom: '1.5rem' }}>
        <h3>Background simulator</h3>
        <p className="muted">
          Demo traffic written to Postgres (CDC → pipeline). Default is one transaction every 60
          seconds.
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
      </section>

      <section className="panel" style={{ marginBottom: '1.5rem' }}>
        <h3>Live monitor data</h3>
        <p className="muted">
          Empty the Live page and pause background simulator traffic. After clearing, only Demo
          scenario clicks add transactions (one scenario at a time).
        </p>
        <button type="button" className="btn" disabled={clearing} onClick={onClearLive}>
          {clearing ? 'Clearing…' : 'Clear all live transactions'}
        </button>
      </section>

      {status && <p className="muted">{status}</p>}
    </section>
  )
}
