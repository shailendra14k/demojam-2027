import { useEffect, useMemo, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { api } from '../api'
import { DetectionBadgeFromEvent } from '../components/DetectionBadge'

const SEVERITY_COLORS: Record<string, string> = {
  CRITICAL: '#f43f5e',
  HIGH: '#fb7185',
  MEDIUM: '#f59e0b',
  LOW: '#2dd4bf',
}

const CHART_TEAL = '#2dd4bf'
const CHART_AMBER = '#f59e0b'
const CHART_MUTED = '#64748b'
const TOOLTIP_STYLE = {
  background: '#0f172a',
  border: '1px solid rgba(148,163,184,0.25)',
  borderRadius: 0,
  fontSize: 12,
  fontFamily: 'IBM Plex Mono, monospace',
}

function useCountUp(target: number, duration = 700) {
  const [value, setValue] = useState(0)
  const fromRef = useRef(0)
  useEffect(() => {
    if (!Number.isFinite(target)) return
    let raf = 0
    const start = performance.now()
    const from = fromRef.current
    const tick = (now: number) => {
      const t = Math.min(1, (now - start) / duration)
      const eased = 1 - Math.pow(1 - t, 3)
      const next = from + (target - from) * eased
      setValue(next)
      if (t < 1) raf = requestAnimationFrame(tick)
      else fromRef.current = target
    }
    raf = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(raf)
  }, [target, duration])
  return value
}

function MetricValue({
  value,
  decimals = 0,
  suffix = '',
}: {
  value: number | null | undefined
  decimals?: number
  suffix?: string
}) {
  const n = typeof value === 'number' && Number.isFinite(value) ? value : 0
  const animated = useCountUp(n)
  const display =
    decimals > 0 ? animated.toFixed(decimals) : Math.round(animated).toLocaleString()
  return (
    <div className="metric">
      {display}
      {suffix}
    </div>
  )
}

function StatusPill({ ok, label }: { ok?: boolean; label: string }) {
  return <span className={`status-pill ${ok ? 'ok' : 'bad'}`}>{label}</span>
}

export default function Dashboard() {
  const [data, setData] = useState<any>(null)
  const [events, setEvents] = useState<any[]>([])
  const [tick, setTick] = useState(0)

  useEffect(() => {
    const load = () => {
      api.summary().then(setData).catch(console.error)
      api
        .events()
        .then((r) => setEvents((r.items || []).slice(0, 8)))
        .catch(console.error)
      setTick((t) => t + 1)
    }
    load()
    const t = setInterval(load, 5000)
    return () => clearInterval(t)
  }, [])

  const stages = data?.pipeline_stages?.length
    ? data.pipeline_stages
    : [
        { name: 'DATABASE', ok: true },
        { name: 'CDC', ok: true },
        { name: 'STREAM', ok: true },
        { name: 'ENRICH', ok: true },
        { name: 'ANALYZE', ok: true },
        { name: 'RISK', ok: true },
        { name: 'ALERT', ok: true },
      ]

  const severityData = useMemo(() => {
    const raw = data?.severity_breakdown || []
    if (raw.length) return raw.map((d: any) => ({ name: d.level, value: d.count }))
    return Object.entries(data?.alerts_by_level || {}).map(([name, value]) => ({
      name,
      value: Number(value),
    }))
  }, [data])

  const volumeData = data?.alert_volume || []
  const histData = data?.risk_histogram || []
  const ruleData = (data?.rule_hit_frequency || []).slice(0, 8)
  const latencySpark = (data?.latency_samples || []).map((v: number, i: number) => ({
    i: i + 1,
    ms: v,
  }))

  const platform = data?.platform || {}
  const platformRows = [
    { key: 'postgres', label: 'PostgreSQL', hint: 'Transactional source' },
    { key: 'debezium', label: 'Debezium CDC', hint: 'Change data capture' },
    { key: 'kafka_connect', label: 'Kafka Connect', hint: 'AMQ Streams connector' },
    { key: 'kafka', label: 'Kafka / AMQ Streams', hint: 'Event backbone' },
    { key: 'feature_engine', label: 'Feature engine', hint: 'Enrichment' },
    { key: 'aml_rules_ml', label: 'Rules + ML', hint: 'ANALYZE stage' },
    { key: 'risk_engine', label: 'Risk engine', hint: 'Score aggregation' },
    { key: 'alert_service', label: 'Alert service', hint: 'Case generation' },
  ]

  const elevated = events.filter(
    (e) => ['HIGH', 'CRITICAL', 'MEDIUM'].includes(String(e.risk?.risk_level || '').toUpperCase()),
  )

  return (
    <div className="ops-console">
      <section className="ops-header">
        <div>
          <p className="ops-eyebrow">AML operations · OpenShift CDC path</p>
          <h2>Investigation console</h2>
          <p className="ops-sub">
            Postgres → Debezium → AMQ Streams → enrich → rules/ML → risk → alert
          </p>
        </div>
        <div className="ops-meta">
          <span className="live-dot" />
          <span className="mono">LIVE · poll {tick}</span>
          <span className="mono muted">
            mode {data?.pipeline_mode ?? '—'} · AI {data?.ai_mode ?? '—'} · up{' '}
            {data?.uptime_seconds ?? 0}s
          </span>
        </div>
      </section>

      <section className="kpi-strip" aria-label="Key performance indicators">
        <div className="kpi">
          <h3>Tx / sec</h3>
          <MetricValue value={data?.tx_per_sec} decimals={3} />
        </div>
        <div className="kpi">
          <h3>Open alerts</h3>
          <MetricValue value={data?.alerts_open} />
        </div>
        <div className="kpi kpi-critical">
          <h3>Critical</h3>
          <MetricValue value={data?.alerts_critical} />
        </div>
        <div className="kpi">
          <h3>High-risk players</h3>
          <MetricValue value={data?.high_risk_players} />
        </div>
        <div className="kpi">
          <h3>Pipeline latency</h3>
          <MetricValue value={data?.latency_ms} suffix=" ms" />
        </div>
        <div className="kpi">
          <h3>AI / ML latency</h3>
          <div className="metric metric-sm">
            {data?.ai_latency_ms ?? '—'}
            <small>AI {data?.ai_latency_ms ?? '—'} · ML {data?.ml_latency_ms ?? '—'} ms</small>
          </div>
        </div>
        <div className="kpi">
          <h3>CDC</h3>
          <StatusPill ok={data?.cdc_ok} label={data?.cdc_ok ? 'OK' : 'DOWN'} />
          <p className="kpi-detail mono">{data?.cdc_detail || '—'}</p>
        </div>
        <div className="kpi">
          <h3>Kafka</h3>
          <StatusPill ok={data?.kafka_ok} label={data?.kafka_ok ? 'OK' : 'DOWN'} />
          <p className="kpi-detail mono">{data?.kafka_detail || '—'}</p>
        </div>
      </section>

      <section className="panel pipeline-panel">
        <div className="panel-head">
          <h3>Pipeline status</h3>
          <Link className="text-link" to="/pipeline">
            Full topology →
          </Link>
        </div>
        <div className="pipeline pipeline-rich">
          {stages.map((s: any, i: number, arr: any[]) => (
            <div key={s.name} className="pipe-unit">
              <div
                className={`pipe-stage ${s.ok === false ? 'bad' : s.ok === true ? 'ok' : ''}`}
              >
                <span className="dot" />
                <div className="pipe-copy">
                  <strong>{s.name}</strong>
                  <span className="mono muted">
                    {s.detail || '—'}
                    {s.activity != null ? ` · ${s.activity}` : ''}
                  </span>
                </div>
              </div>
              {i < arr.length - 1 && <span className="pipe-arrow" aria-hidden />}
            </div>
          ))}
        </div>
      </section>

      <div className="grid ops-grid">
        <div className="panel span-4">
          <div className="panel-head">
            <h3>Alert volume</h3>
            <span className="muted mono">time series</span>
          </div>
          <div className="chart-box">
            {volumeData.length ? (
              <ResponsiveContainer width="100%" height={200}>
                <AreaChart data={volumeData}>
                  <defs>
                    <linearGradient id="volFill" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={CHART_TEAL} stopOpacity={0.35} />
                      <stop offset="100%" stopColor={CHART_TEAL} stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <XAxis dataKey="t" tick={{ fill: CHART_MUTED, fontSize: 11 }} axisLine={false} tickLine={false} />
                  <YAxis tick={{ fill: CHART_MUTED, fontSize: 11 }} axisLine={false} tickLine={false} width={28} />
                  <Tooltip contentStyle={TOOLTIP_STYLE} />
                  <Area type="monotone" dataKey="count" stroke={CHART_TEAL} fill="url(#volFill)" strokeWidth={2} />
                </AreaChart>
              </ResponsiveContainer>
            ) : (
              <p className="muted">No volume series yet.</p>
            )}
          </div>
        </div>

        <div className="panel span-4">
          <div className="panel-head">
            <h3>Severity mix</h3>
            <span className="muted mono">risk level</span>
          </div>
          <div className="chart-box chart-split">
            {severityData.length ? (
              <>
                <ResponsiveContainer width="48%" height={200}>
                  <PieChart>
                    <Pie
                      data={severityData}
                      dataKey="value"
                      nameKey="name"
                      innerRadius={48}
                      outerRadius={72}
                      paddingAngle={2}
                      stroke="none"
                    >
                      {severityData.map((d: any) => (
                        <Cell key={d.name} fill={SEVERITY_COLORS[d.name] || CHART_TEAL} />
                      ))}
                    </Pie>
                    <Tooltip contentStyle={TOOLTIP_STYLE} />
                  </PieChart>
                </ResponsiveContainer>
                <ul className="legend-list">
                  {severityData.map((d: any) => (
                    <li key={d.name}>
                      <span className="swatch" style={{ background: SEVERITY_COLORS[d.name] || CHART_TEAL }} />
                      <span>{d.name}</span>
                      <strong className="mono">{d.value}</strong>
                    </li>
                  ))}
                </ul>
              </>
            ) : (
              <p className="muted">No alerts yet — run a demo scenario.</p>
            )}
          </div>
        </div>

        <div className="panel span-4">
          <div className="panel-head">
            <h3>Risk score histogram</h3>
            <span className="muted mono">0–1</span>
          </div>
          <div className="chart-box">
            {histData.length ? (
              <ResponsiveContainer width="100%" height={200}>
                <BarChart data={histData}>
                  <XAxis dataKey="bucket" tick={{ fill: CHART_MUTED, fontSize: 10 }} axisLine={false} tickLine={false} />
                  <YAxis tick={{ fill: CHART_MUTED, fontSize: 11 }} axisLine={false} tickLine={false} width={28} />
                  <Tooltip contentStyle={TOOLTIP_STYLE} />
                  <Bar dataKey="count" fill={CHART_AMBER} radius={[0, 0, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <p className="muted">No score distribution yet.</p>
            )}
          </div>
        </div>

        <div className="panel span-6">
          <div className="panel-head">
            <h3>Rule hit frequency</h3>
            <span className="muted mono">AML-001…008</span>
          </div>
          <div className="chart-box">
            {ruleData.length ? (
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={ruleData} layout="vertical" margin={{ left: 12 }}>
                  <XAxis type="number" tick={{ fill: CHART_MUTED, fontSize: 11 }} axisLine={false} tickLine={false} />
                  <YAxis
                    type="category"
                    dataKey="rule_id"
                    width={72}
                    tick={{ fill: '#cbd5e1', fontSize: 11, fontFamily: 'IBM Plex Mono' }}
                    axisLine={false}
                    tickLine={false}
                  />
                  <Tooltip contentStyle={TOOLTIP_STYLE} />
                  <Bar dataKey="count" fill={CHART_TEAL} barSize={14} />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <p className="muted">No rule hits yet.</p>
            )}
          </div>
        </div>

        <div className="panel span-6">
          <div className="panel-head">
            <h3>Latency samples</h3>
            <span className="muted mono">pipeline ms</span>
          </div>
          <div className="chart-box">
            {latencySpark.length ? (
              <ResponsiveContainer width="100%" height={220}>
                <AreaChart data={latencySpark}>
                  <XAxis dataKey="i" hide />
                  <YAxis tick={{ fill: CHART_MUTED, fontSize: 11 }} axisLine={false} tickLine={false} width={36} />
                  <Tooltip contentStyle={TOOLTIP_STYLE} />
                  <Area type="stepAfter" dataKey="ms" stroke="#38bdf8" fill="rgba(56,189,248,0.15)" strokeWidth={2} />
                </AreaChart>
              </ResponsiveContainer>
            ) : (
              <p className="muted">Latency samples unavailable.</p>
            )}
          </div>
        </div>

        <div className="panel span-7">
          <div className="panel-head">
            <h3>Live feed · elevated</h3>
            <Link className="text-link" to="/live">
              Open Live Monitor →
            </Link>
          </div>
          <div className="feed-list">
            {(elevated.length ? elevated : events).slice(0, 6).map((e, i) => (
              <div key={e.event?.event_id || i} className="feed-row" style={{ animationDelay: `${i * 60}ms` }}>
                <DetectionBadgeFromEvent row={e} />
                <span className="mono muted">{String(e.event?.timestamp || '').slice(11, 19)}</span>
                <Link className="mono" to={`/player/${e.event?.player_id || ''}`}>
                  {String(e.event?.player_id || '—').slice(0, 8)}
                </Link>
                <span className="mono">{e.risk?.final_score?.toFixed?.(2) ?? '—'}</span>
                <span className="feed-rules mono muted">
                  {(e.rules?.hits || e.risk?.rule_hits || []).map((h: any) => h.rule_id).join(' ') || '—'}
                </span>
              </div>
            ))}
            {!events.length && <p className="muted">Waiting for scored CDC events…</p>}
          </div>
        </div>

        <div className="panel span-5">
          <div className="panel-head">
            <h3>Platform health</h3>
            <span className="muted mono">
              model {platform.model_loaded ? 'loaded' : 'missing'}
            </span>
          </div>
          <ul className="health-grid">
            {platformRows.map((row) => {
              const node = platform[row.key] || {}
              const ok = Boolean(node.ok)
              return (
                <li key={row.key} className={ok ? 'ok' : 'bad'}>
                  <span className="health-dot" />
                  <div>
                    <strong>{row.label}</strong>
                    <span className="muted">{row.hint}</span>
                  </div>
                  <span className="mono health-detail">{node.detail || (ok ? 'ok' : 'down')}</span>
                </li>
              )
            })}
          </ul>
        </div>
      </div>
    </div>
  )
}
