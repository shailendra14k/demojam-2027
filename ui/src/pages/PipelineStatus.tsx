import { useEffect, useMemo, useState } from 'react'
import { api } from '../api'

type FlowNode = {
  id: string
  label: string
  kind: string
  ok: boolean
  status: string
  detail?: string
  activity?: number
  flowing?: boolean
  optional?: boolean
}

type FlowEdge = { from: string; to: string; flowing?: boolean }

/**
 * Free-form topology (% of canvas). Zigzag / river — not a single horizontal line.
 * x/y are node centers.
 */
const LAYOUT: Record<string, { x: number; y: number }> = {
  db: { x: 8, y: 20 },
  cdc: { x: 16, y: 62 },
  kafka: { x: 27, y: 28 },
  normalizer: { x: 38, y: 68 },
  features: { x: 48, y: 30 },
  rules: { x: 58, y: 10 },
  ml: { x: 58, y: 42 },
  tfm: { x: 58, y: 78 },
  risk: { x: 70, y: 42 },
  alerts: { x: 80, y: 18 },
  openjev: { x: 80, y: 48 },
  api: { x: 90, y: 32 },
  ui: { x: 94, y: 78 },
}

const KIND_GLYPH: Record<string, string> = {
  storage: '◆',
  ingest: '◎',
  bus: '≋',
  service: '●',
  gpu: '✦',
  decide: '⬡',
  api: '◇',
  ui: '▣',
}

const LANES = [
  { label: 'Ingest', x: 12 },
  { label: 'Stream', x: 32 },
  { label: 'Enrich', x: 48 },
  { label: 'Analyze', x: 58 },
  { label: 'Score', x: 70 },
  { label: 'Disposition', x: 80 },
  { label: 'Serve', x: 92 },
]

function StatusBadge({ ok, optional }: { ok: boolean; optional?: boolean }) {
  if (optional && ok) {
    return <span className="flow-badge idle">standby</span>
  }
  return (
    <span className={`flow-badge ${ok ? 'ok' : 'bad'}`}>{ok ? 'healthy' : 'down'}</span>
  )
}

function curvePath(
  a: { x: number; y: number },
  b: { x: number; y: number },
): string {
  const dx = b.x - a.x
  const dy = b.y - a.y
  // Soft cubic — bend more on vertical fans (rules/ml/tfm)
  const lift = Math.abs(dy) > 12 ? dy * 0.15 : dy * 0.05
  const c1x = a.x + dx * 0.35
  const c1y = a.y + lift
  const c2x = a.x + dx * 0.65
  const c2y = b.y - lift
  return `M ${a.x} ${a.y} C ${c1x} ${c1y}, ${c2x} ${c2y}, ${b.x} ${b.y}`
}

export default function PipelineStatus() {
  const [data, setData] = useState<any>(null)

  useEffect(() => {
    const load = () => api.pipeline().then(setData).catch(console.error)
    load()
    const t = setInterval(load, 4000)
    return () => clearInterval(t)
  }, [])

  const components: FlowNode[] = data?.components || []
  const edges: FlowEdge[] = data?.edges || []
  const stages = data?.stages || []

  const byId = useMemo(() => {
    const m = new Map<string, FlowNode>()
    for (const c of components) m.set(c.id, c)
    return m
  }, [components])

  const visibleNodes = useMemo(
    () =>
      components.filter((c) => {
        if (c.id === 'openjev' && data?.openjev?.mode === 'off') return false
        return true
      }),
    [components, data?.openjev?.mode],
  )

  const edgePaths = useMemo(() => {
    return edges
      .filter((e) => byId.has(e.from) && byId.has(e.to))
      .map((e) => {
        const a = LAYOUT[e.from] || { x: 10, y: 50 }
        const b = LAYOUT[e.to] || { x: 90, y: 50 }
        const src = byId.get(e.from)
        return {
          ...e,
          d: curvePath(a, b),
          kind: src?.kind || 'service',
        }
      })
  }, [edges, byId])

  const healthy = visibleNodes.filter((n) => n.ok).length
  const down = visibleNodes.filter((n) => !n.ok).length
  const flowingCount = edges.filter((e) => e.flowing).length

  return (
    <div className="pipeline-page">
      <section className="hero">
        <h2>Pipeline status</h2>
        <p>
          Live topology on OpenShift — component health and traffic through the Red Hat AML stack.
        </p>
      </section>

      <div className="panel flow-legend-bar">
        <div className="flow-kpis">
          <span>
            <strong className="ok-ink">{healthy}</strong> healthy
          </span>
          <span>
            <strong className="bad-ink">{down}</strong> down
          </span>
          <span>
            <strong>{flowingCount}</strong> active hops
          </span>
          <span className="muted mono">
            mode {data?.pipeline_mode ?? '—'} · AI {data?.ai_mode ?? '—'} · TFM{' '}
            {data?.tfm?.mode ?? '—'} · OpenJev {data?.openjev?.mode ?? '—'}
            {data?.openjev?.model ? `/${data.openjev.model}` : ''}
          </span>
        </div>
        <div className="flow-legend">
          <span>
            <i className="flow-swatch ok" /> healthy
          </span>
          <span>
            <i className="flow-swatch bad" /> down
          </span>
          <span>
            <i className="flow-swatch flow" /> traffic flowing
          </span>
          <span>
            <i className="flow-swatch idle" /> idle / standby
          </span>
        </div>
      </div>

      <div className="panel flow-diagram-wrap">
        <div className="flow-diagram" aria-label="AML pipeline flow diagram">
          <div className="flow-lanes" aria-hidden>
            {LANES.map((lane) => (
              <div key={lane.label} className="flow-lane" style={{ left: `${lane.x}%` }}>
                <span>{lane.label}</span>
              </div>
            ))}
          </div>

          <svg className="flow-edges" viewBox="0 0 100 100" preserveAspectRatio="none">
            <defs>
              <linearGradient id="flowGradTeal" x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stopColor="#ee0000" stopOpacity="0.2" />
                <stop offset="50%" stopColor="#ee0000" stopOpacity="1" />
                <stop offset="100%" stopColor="#2b9af3" stopOpacity="0.85" />
              </linearGradient>
              <linearGradient id="flowGradViolet" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" stopColor="#73c5c5" stopOpacity="0.9" />
                <stop offset="100%" stopColor="#c46100" stopOpacity="0.85" />
              </linearGradient>
              <linearGradient id="flowGradAmber" x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stopColor="#f0ab00" stopOpacity="0.85" />
                <stop offset="100%" stopColor="#2b9af3" stopOpacity="0.9" />
              </linearGradient>
            </defs>
            {edgePaths.map((e) => (
              <path
                key={`${e.from}-${e.to}`}
                d={e.d}
                className={`flow-edge kind-${e.kind} ${e.flowing ? 'flowing' : 'idle'}`}
              />
            ))}
          </svg>

          <div className="flow-canvas">
            {visibleNodes.map((n) => {
              const L = LAYOUT[n.id] || { x: 10, y: 50 }
              const state = !n.ok ? 'bad' : n.flowing ? 'flowing' : n.optional ? 'standby' : 'ok'
              return (
                <article
                  key={n.id}
                  className={`flow-node kind-${n.kind} ${state}`}
                  style={{ left: `${L.x}%`, top: `${L.y}%` }}
                  title={n.detail || n.label}
                >
                  <div className="flow-node-glow" aria-hidden />
                  <header>
                    <span className={`flow-glyph kind-${n.kind}`} aria-hidden>
                      {KIND_GLYPH[n.kind] || '●'}
                    </span>
                    <StatusBadge ok={n.ok} optional={n.optional && n.ok && !n.flowing} />
                  </header>
                  <h3>{n.label}</h3>
                  <p className="mono muted flow-detail">{n.detail || n.kind}</p>
                  {typeof n.activity === 'number' && n.activity > 0 && (
                    <p className="mono flow-activity">activity {n.activity}</p>
                  )}
                  {n.flowing && <div className="flow-pulse" aria-hidden />}
                </article>
              )
            })}
          </div>
        </div>
        <p className="muted flow-footnote">
          Uptime {data?.uptime_seconds ?? 0}s · buffered events {data?.events_buffered ?? 0} · open
          alerts {data?.open_alerts ?? 0}
          {data?.note ? ` · ${data.note}` : ''}
        </p>
      </div>

      <div className="panel">
        <div className="panel-head">
          <h3>Stage strip</h3>
          <span className="muted mono">DATABASE → … → ALERT</span>
        </div>
        <div className="pipeline pipeline-rich">
          {(stages.length
            ? stages
            : [
                { name: 'DATABASE' },
                { name: 'CDC' },
                { name: 'STREAM' },
                { name: 'ENRICH' },
                { name: 'ANALYZE' },
                { name: 'RISK' },
                { name: 'ALERT' },
              ]
          ).map((s: any, i: number, arr: any[]) => (
            <div key={s.name} className="pipe-unit">
              <div className={`pipe-stage ${s.ok === false ? 'bad' : s.ok === true ? 'ok' : ''}`}>
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
      </div>
    </div>
  )
}
