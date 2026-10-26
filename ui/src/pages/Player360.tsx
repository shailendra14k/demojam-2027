import { useEffect, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api } from '../api'
import { DetectionBadgeFromAlert } from '../components/DetectionBadge'
import { formatRuleNames } from '../lib/alertLabels'

function money(v: unknown) {
  if (v == null || v === '') return '—'
  const n = Number(v)
  return Number.isFinite(n) ? n.toLocaleString(undefined, { maximumFractionDigits: 2 }) : String(v)
}

function when(v: unknown) {
  if (!v) return '—'
  const s = String(v)
  return s.length > 19 ? s.slice(0, 19).replace('T', ' ') : s
}

export default function Player360() {
  const { id: routeId } = useParams()
  const navigate = useNavigate()
  const [id, setId] = useState(routeId || '')
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  const loadPlayer = (pid: string) => {
    if (!pid.trim()) return
    setLoading(true)
    setError(null)
    api
      .player(pid.trim())
      .then((d) => {
        setData(d)
        const resolved = d?.profile?.player_id || pid.trim()
        setId(resolved)
      })
      .catch((e) => {
        setData(null)
        setError(String(e?.message || e))
      })
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    if (!routeId) {
      api
        .alerts()
        .then((r) => {
          const first = r.items?.[0]?.player_id
          if (first) {
            setId(first)
            loadPlayer(first)
          }
        })
        .catch(console.error)
    } else {
      setId(routeId)
      loadPlayer(routeId)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [routeId])

  const load = () => {
    if (!id.trim()) return
    navigate(`/player/${encodeURIComponent(id.trim())}`)
    loadPlayer(id.trim())
  }

  const p = data?.profile
  const kyc = data?.kyc

  return (
    <div>
      <section className="hero">
        <h2>Player 360</h2>
        <p>Unified view of KYC, payments, wagering, devices, and open alerts for a player.</p>
      </section>
      <div className="panel stack" style={{ marginBottom: '1rem' }}>
        <div className="btn-row">
          <input
            className="mono"
            style={{
              flex: 1,
              minWidth: 240,
              padding: '0.55rem',
              background: '#ffffff',
              color: '#151515',
              border: '1px solid var(--line)',
            }}
            value={id}
            onChange={(e) => setId(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && load()}
            placeholder="player UUID, EXT-ref, or 8-char prefix"
          />
          <button className="btn" onClick={load} disabled={loading}>
            {loading ? 'Loading…' : 'Load'}
          </button>
        </div>
        {error && <p className="muted" style={{ color: 'var(--danger)' }}>{error}</p>}
        {data?.source === 'memory' && data?.detail && (
          <p className="muted">{data.detail}</p>
        )}
      </div>

      {data && p && (
        <div className="grid">
          <div className="panel span-4">
            <h3>Profile</h3>
            <div className="mono">{p.player_id}</div>
            {(p.first_name || p.last_name) && (
              <div style={{ fontSize: '1.15rem', marginTop: '0.35rem' }}>
                {p.first_name} {p.last_name}
              </div>
            )}
            {p.external_ref && <div className="muted mono">{p.external_ref}</div>}
            {p.email && <div className="muted">{p.email}</div>}
            <div>Status: {p.status || '—'}</div>
            <div>Nationality: {p.nationality || '—'}</div>
            <div>DOB: {p.date_of_birth || '—'}</div>
            <div>Activity rows: {p.events ?? '—'}</div>
            <div>Alerts: {p.alerts ?? '—'}</div>
            <div>
              Last risk:{' '}
              <span className={`badge ${p.last_risk}`}>{p.last_risk || '—'}</span>
            </div>
            <div>
              Last score: <span className="mono">{p.last_score ?? '—'}</span>
            </div>
            {p.wallet && (
              <div style={{ marginTop: '0.75rem' }}>
                <h3>Wallet</h3>
                <div className="mono">
                  {money(p.wallet.balance)} {p.wallet.currency}
                </div>
                <div className="muted">Available: {money(p.wallet.available_balance)}</div>
              </div>
            )}
          </div>

          <div className="panel span-4">
            <h3>KYC</h3>
            {!kyc && <p className="muted">No KYC profile on file.</p>}
            {kyc && (
              <div className="stack">
                <div>
                  Tier: <strong>{kyc.kyc_tier}</strong> · {kyc.verification_status}
                </div>
                <div>
                  Risk rating:{' '}
                  <span className={`badge ${kyc.risk_rating}`}>{kyc.risk_rating}</span>
                </div>
                <div>
                  Document: {kyc.document_type || '—'} ({kyc.document_country || '—'})
                </div>
                <div>PEP: {kyc.is_pep ? 'YES' : 'no'} · Sanctioned: {kyc.is_sanctioned ? 'YES' : 'no'}</div>
                <div className="muted">Verified: {when(kyc.verified_at)}</div>
              </div>
            )}
          </div>

          <div className="panel span-4">
            <h3>Locations</h3>
            {(data.locations || []).length === 0 && <p className="muted">No locations recorded.</p>}
            <ul>
              {(data.locations || []).map((loc: string) => (
                <li key={loc}>{loc}</li>
              ))}
            </ul>
            <h3 style={{ marginTop: '1rem' }}>Devices</h3>
            {(data.devices || []).length === 0 && <p className="muted">No devices.</p>}
            <table>
              <thead>
                <tr>
                  <th>Type</th>
                  <th>Fingerprint</th>
                  <th>Trusted</th>
                </tr>
              </thead>
              <tbody>
                {(data.devices || []).map((d: any) => (
                  <tr key={d.device_id}>
                    <td>{d.device_type}</td>
                    <td className="mono">{String(d.device_fingerprint || '').slice(0, 12)}</td>
                    <td>{d.is_trusted ? 'yes' : 'no'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="panel span-6">
            <h3>Payment methods</h3>
            {(data.payment_methods || []).length === 0 && <p className="muted">None on file.</p>}
            <table>
              <thead>
                <tr>
                  <th>Type</th>
                  <th>Provider</th>
                  <th>Last4</th>
                  <th>Verified</th>
                </tr>
              </thead>
              <tbody>
                {(data.payment_methods || []).map((m: any) => (
                  <tr key={m.payment_method_id}>
                    <td>{m.method_type}</td>
                    <td>{m.provider || '—'}</td>
                    <td className="mono">{m.last4 || '—'}</td>
                    <td>{m.is_verified ? 'yes' : 'no'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="panel span-6">
            <h3>Alerts</h3>
            {(data.alerts || []).length === 0 && <p className="muted">No alerts.</p>}
            <table>
              <thead>
                <tr>
                  <th>Detection</th>
                  <th>Status</th>
                  <th>Severity</th>
                  <th>Score</th>
                </tr>
              </thead>
              <tbody>
                {(data.alerts || []).map((a: any) => (
                  <tr key={a.alert_id}>
                    <td>
                      <Link to={`/live?case=${encodeURIComponent(a.alert_id)}`}>
                        <DetectionBadgeFromAlert alert={a} />
                      </Link>
                    </td>
                    <td>{a.status}</td>
                    <td>
                      <span className={`badge ${a.risk_level}`}>{a.risk_level}</span>
                    </td>
                    <td className="mono">{a.score?.toFixed?.(2) ?? a.score}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="panel span-6">
            <h3>Deposits / withdrawals</h3>
            <table>
              <thead>
                <tr>
                  <th>When</th>
                  <th>Type</th>
                  <th>Amount</th>
                  <th>Location</th>
                </tr>
              </thead>
              <tbody>
                {[...(data.deposits || []), ...(data.withdrawals || [])]
                  .sort((a: any, b: any) => String(b.created_at).localeCompare(String(a.created_at)))
                  .slice(0, 25)
                  .map((t: any) => (
                    <tr key={t.transaction_id}>
                      <td className="mono">{when(t.created_at)}</td>
                      <td>{t.transaction_type}</td>
                      <td className="mono">{money(t.amount)}</td>
                      <td>{t.location || '—'}</td>
                    </tr>
                  ))}
              </tbody>
            </table>
            {(data.deposits || []).length + (data.withdrawals || []).length === 0 && (
              <p className="muted">No deposits or withdrawals.</p>
            )}
          </div>

          <div className="panel span-6">
            <h3>All transactions</h3>
            <table>
              <thead>
                <tr>
                  <th>When</th>
                  <th>Type</th>
                  <th>Amount</th>
                  <th>Channel</th>
                </tr>
              </thead>
              <tbody>
                {(data.transactions || []).slice(0, 25).map((t: any) => (
                  <tr key={t.transaction_id}>
                    <td className="mono">{when(t.created_at)}</td>
                    <td>{t.transaction_type}</td>
                    <td className="mono">{money(t.amount)}</td>
                    <td>{t.channel || '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {(data.transactions || []).length === 0 && <p className="muted">No transactions.</p>}
          </div>

          <div className="panel span-6">
            <h3>Wagers</h3>
            <table>
              <thead>
                <tr>
                  <th>When</th>
                  <th>Game</th>
                  <th>Stake</th>
                  <th>Payout</th>
                </tr>
              </thead>
              <tbody>
                {(data.wagers || []).slice(0, 25).map((w: any) => (
                  <tr key={w.wager_id}>
                    <td className="mono">{when(w.created_at)}</td>
                    <td>{w.game_type}</td>
                    <td className="mono">{money(w.stake)}</td>
                    <td className="mono">{money(w.payout)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {(data.wagers || []).length === 0 && <p className="muted">No wagers.</p>}
          </div>

          <div className="panel span-6">
            <h3>Chip / cage</h3>
            <table>
              <thead>
                <tr>
                  <th>When</th>
                  <th>Source</th>
                  <th>Type</th>
                  <th>Amount</th>
                </tr>
              </thead>
              <tbody>
                {(data.chips || []).slice(0, 15).map((c: any) => (
                  <tr key={c.chip_tx_id}>
                    <td className="mono">{when(c.created_at)}</td>
                    <td>CHIP</td>
                    <td>{c.tx_type}</td>
                    <td className="mono">{money(c.amount)}</td>
                  </tr>
                ))}
                {(data.cage || []).slice(0, 15).map((c: any) => (
                  <tr key={c.cage_tx_id}>
                    <td className="mono">{when(c.created_at)}</td>
                    <td>CAGE</td>
                    <td>{c.tx_type}</td>
                    <td className="mono">{money(c.amount)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {(data.chips || []).length + (data.cage || []).length === 0 && (
              <p className="muted">No chip or cage activity.</p>
            )}
          </div>

          <div className="panel span-12">
            <h3>Recent scored events</h3>
            <table>
              <thead>
                <tr>
                  <th>Type</th>
                  <th>Amount</th>
                  <th>Risk</th>
                  <th>Rules</th>
                </tr>
              </thead>
              <tbody>
                {(data.recent_events || []).map((e: any, i: number) => (
                  <tr key={e.event?.event_id || e.event?.transaction_id || i}>
                    <td>{e.event?.transaction_type || e.event?.event_type}</td>
                    <td className="mono">{e.event?.amount}</td>
                    <td>
                      <span className={`badge ${e.risk?.risk_level}`}>{e.risk?.risk_level}</span>
                    </td>
                    <td>{formatRuleNames(e.rules?.hits)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {(data.recent_events || []).length === 0 && (
              <p className="muted">No scored events in the live buffer for this player.</p>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
