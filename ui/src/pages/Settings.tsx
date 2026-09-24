import { useEffect, useState } from 'react'
import { api } from '../api'

export default function Settings() {
  const [w, setW] = useState<any>(null)

  useEffect(() => {
    api.weights().then(setW).catch(console.error)
  }, [])

  return (
    <section>
      <h2>Scoring weights</h2>
      <p className="muted">Live ConfigMap / environment values (not hardcoded demo copy).</p>
      {!w && <p className="muted">Loading…</p>}
      {w && (
        <ul>
          <li>Rules: <strong>{w.rules}</strong></li>
          <li>ML (XGBoost): <strong>{w.ml}</strong></li>
          <li>Behavioral: <strong>{w.behavioral}</strong></li>
          <li>KYC (optional): <strong>{w.kyc}</strong></li>
          <li>Alert threshold: <strong>{w.alert_threshold}</strong></li>
        </ul>
      )}
      {w?.note && <p className="muted">{w.note}</p>}
    </section>
  )
}
