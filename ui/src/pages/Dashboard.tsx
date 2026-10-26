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
import { formatRuleNames, ruleDisplayName } from '../lib/alertLabels'

const SEVERITY_COLORS: Record<string, string> = {
  CRITICAL: '#c9190b',
  HIGH: '#ee0000',
  MEDIUM: '#f0ab00',
  LOW: '#3e8635',
}

const CHART_TEAL = '#ee0000'
const CHART_MUTED = '#6a6e73'
const TOOLTIP_STYLE = {
  background: '#ffffff',
  border: '1px solid rgba(21,21,21,0.15)',
  color: '#151515',
  borderRadius: 0,
  fontSize: 12,
  fontFamily: 'Red Hat Display, sans-serif',
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
}: {
  value: number | null | undefined
  decimals?: number
}) {
  const n = typeof value === 'number' && Number.isFinite(value) ? value : 0
  const animated = useCountUp(n)
  const display =
    decimals > 0 ? animated.toFixed(decimals) : Math.round(animated).toLocaleString()
  return <div className="metric">{display}</div>
}

export default function Dashboard() {
  const [data, setData] = useState<any>(null)
  const [events, setEvents] = useState<any[]>([])
  const [industry, setIndustry] = useState<any>(null)
  const [tick, setTick] = useState(0)

  useEffect(() => {
    const load = () => {
      api.summary().then(setData).catch(console.error)
      api
        .events()
        .then((r) => setEvents((r.items || []).slice(0, 8)))
        .catch(console.error)
      api.industrySettings().then(setIndustry).catch(() => undefined)
      setTick((t) => t + 1)
    }
    load()
    const t = setInterval(load, 5000)
    return () => clearInterval(t)
  }, [])

  const severityData = useMemo(() => {
    const raw = data?.severity_breakdown || []
    if (raw.length) return raw.map((d: any) => ({ name: d.level, value: d.count }))
    return Object.entries(data?.alerts_by_level || {}).map(([name, value]) => ({
      name,
      value: Number(value),
    }))
  }, [data])

  const volumeData = data?.alert_volume || []
  const ruleData = useMemo(
    () =>
      (data?.rule_hit_frequency || []).slice(0, 8).map((d: any) => ({
        ...d,
        rule_name: d.rule_name || ruleDisplayName(d.rule_id || d),
      })),
    [data],
  )

  const elevated = events.filter((e) =>
    ['HIGH', 'CRITICAL', 'MEDIUM'].includes(String(e.risk?.risk_level || '').toUpperCase()),
  )
  const pipeOk = data?.cdc_ok !== false && data?.kafka_ok !== false

  return (
    <div className="ops-console">
      <section className="ops-header">
        <div>
          <p className="ops-eyebrow">Realtime TxnIQ · pattern intelligence</p>
          <h2>Operations console</h2>
          <p className="ops-sub">
            Patterns over the stream — not one-shot flags.
            {industry?.label ? (
              <>
                {' '}
                Active pack: <strong>{industry.label}</strong>
              </>
            ) : null}
          </p>
        </div>
        <div className="ops-meta">
          <span className="live-dot" />
          <span className="mono">LIVE · poll {tick}</span>
          <span className={`status-pill ${pipeOk ? 'ok' : 'bad'}`}>
            {pipeOk ? 'Pipeline OK' : 'Pipeline issue'}
            {data?.latency_ms != null ? ` · ${data.latency_ms} ms` : ''}
          </span>
          <Link className="text-link" to="/pipeline">
            Topology →
          </Link>
        </div>
      </section>

      <section className="kpi-strip" aria-label="Key performance indicators">
        <div className="kpi">
          <h3>Open alerts</h3>
          <MetricValue value={data?.alerts_open} />
        </div>
        <div className="kpi kpi-critical">
          <h3>Critical</h3>
          <MetricValue value={data?.alerts_critical} />
        </div>
        <div className="kpi">
          <h3>High-risk entities</h3>
          <MetricValue value={data?.high_risk_players} />
        </div>
        <div className="kpi">
          <h3>CDC · Kafka</h3>
          <div className="metric metric-sm">
            <span className={`status-pill ${data?.cdc_ok ? 'ok' : 'bad'}`}>
              {data?.cdc_ok ? 'CDC OK' : 'CDC'}
            </span>{' '}
            <span className={`status-pill ${data?.kafka_ok ? 'ok' : 'bad'}`}>
              {data?.kafka_ok ? 'Kafka OK' : 'Kafka'}
            </span>
          </div>
        </div>
      </section>

      <div className="grid ops-grid">
        <div className="panel span-4">
          <div className="panel-head">
            <h3>Alert volume</h3>
            <span className="muted mono">over time</span>
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
            <span className="muted mono">queue</span>
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
            <h3>Pattern rules firing</h3>
            <span className="muted mono">top hits</span>
          </div>
          <div className="chart-box">
            {ruleData.length ? (
              <ResponsiveContainer width="100%" height={200}>
                <BarChart data={ruleData} layout="vertical" margin={{ left: 8 }}>
                  <XAxis type="number" tick={{ fill: CHART_MUTED, fontSize: 11 }} axisLine={false} tickLine={false} />
                  <YAxis
                    type="category"
                    dataKey="rule_name"
                    width={120}
                    tick={{ fill: '#6a6e73', fontSize: 11, fontFamily: 'Red Hat Display' }}
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

        <div className="panel span-12">
          <div className="panel-head">
            <h3>Elevated patterns</h3>
            <Link className="text-link" to="/live">
              Open Live →
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
                <span className="feed-rules muted">
                  {formatRuleNames(e.rules?.hits || e.risk?.rule_hits || [], ' · ')}
                </span>
              </div>
            ))}
            {!events.length && (
              <p className="muted">Live is empty — clear leaves it blank; Demo injects one scenario at a time.</p>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
