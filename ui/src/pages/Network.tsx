import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api } from '../api'
import EntityGraph from '../components/EntityGraph'

type Investigation = {
  investigation_id: string
  title?: string
  priority?: string
  status?: string
  score?: number
  player_id?: string
  created_at?: string
  rule_hits?: string[]
}

type NetworkPayload = {
  investigation_id: string
  title?: string
  risk_level?: string
  score?: number
  status?: string
  player_id?: string
  generated_at?: string
  nodes: any[]
  edges: any[]
  records: any[]
  hidden_relations: any[]
  legend: { kind: string; label: string }[]
}

function shortId(id?: string) {
  if (!id) return '—'
  return id.length > 14 ? `${id.slice(0, 12)}…` : id
}

function when(v?: string) {
  if (!v) return '—'
  return String(v).slice(0, 19).replace('T', ' ')
}

export default function Network() {
  const { investigationId } = useParams()
  const navigate = useNavigate()
  const [list, setList] = useState<Investigation[]>([])
  const [graph, setGraph] = useState<NetworkPayload | null>(null)
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [livePulse, setLivePulse] = useState(0)

  const loadList = useCallback(async () => {
    const r = await api.networkInvestigations()
    setList(r.items || [])
    return r.items || []
  }, [])

  const loadGraph = useCallback(async (id: string) => {
    setLoading(true)
    setErr(null)
    try {
      const g = await api.investigationNetwork(id)
      setGraph(g)
      setLivePulse((n) => n + 1)
      const primary =
        g.nodes?.find((n: any) => n.kind === 'alert')?.id ||
        g.nodes?.find((n: any) => n.props?.primary)?.id ||
        g.nodes?.[0]?.id ||
        null
      setSelectedId((prev) => (prev && g.nodes?.some((n: any) => n.id === prev) ? prev : primary))
    } catch (e: any) {
      setErr(String(e?.message || e))
      setGraph(null)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    loadList().catch((e) => setErr(String(e)))
  }, [loadList])

  useEffect(() => {
    if (!investigationId) {
      setGraph(null)
      return
    }
    loadGraph(investigationId).catch(console.error)
    const t = setInterval(() => loadGraph(investigationId).catch(console.error), 10000)
    return () => clearInterval(t)
  }, [investigationId, loadGraph])

  // Auto-open highest priority investigation when landing on /network
  useEffect(() => {
    if (investigationId || list.length === 0) return
    const first = list[0]
    if (first?.investigation_id) {
      navigate(`/network/${first.investigation_id}`, { replace: true })
    }
  }, [investigationId, list, navigate])

  const selectedNode = useMemo(
    () => graph?.nodes?.find((n) => n.id === selectedId) || null,
    [graph, selectedId],
  )

  const openInvestigation = (id: string) => navigate(`/network/${id}`)

  return (
    <div className="net-workspace">
      <section className="hero net-hero">
        <div>
          <p className="ops-eyebrow">Entity generation · hidden relation detection</p>
          <h2>Network investigation</h2>
          <p>
            Open an investigation ID to expand players, devices, payments, and predicted mule links.
            Amber edges are real-time hidden gaps the system filled from shared instruments /
            correlation.
          </p>
        </div>
        <div className="ops-meta">
          <span className="live-dot" aria-hidden />
          <span>
            LIVE · poll {livePulse} · {graph?.nodes?.length || 0} entities ·{' '}
            {graph?.hidden_relations?.length || 0} hidden links
          </span>
        </div>
      </section>

      <div className="net-tabs" role="tablist" aria-label="Open investigations">
        {list.length === 0 && (
          <span className="muted">No open investigations — run a demo scenario first.</span>
        )}
        {list.slice(0, 12).map((inv) => {
          const active = inv.investigation_id === investigationId
          return (
            <button
              key={inv.investigation_id}
              type="button"
              role="tab"
              aria-selected={active}
              className={`net-tab ${active ? 'active' : ''}`}
              onClick={() => openInvestigation(inv.investigation_id)}
            >
              <span className={`badge ${inv.priority}`}>{inv.priority}</span>
              <span className="mono">{shortId(inv.investigation_id)}</span>
            </button>
          )
        })}
      </div>

      {err && <p className="error">{err}</p>}
      {loading && !graph && <p className="muted">Building entity graph…</p>}

      {graph && (
        <>
          <div className="net-subnav">
            <div>
              <strong>{graph.title}</strong>
              <div className="mono muted">
                Investigation {graph.investigation_id} · score{' '}
                {typeof graph.score === 'number' ? graph.score.toFixed(4) : graph.score} ·{' '}
                <span className={`badge ${graph.risk_level}`}>{graph.risk_level}</span> ·{' '}
                {graph.status}
              </div>
            </div>
            <div className="btn-row">
              {graph.player_id && (
                <Link className="btn secondary" to={`/player/${graph.player_id}`}>
                  Player 360
                </Link>
              )}
              <Link
                className="btn secondary"
                to={graph.investigation_id ? `/live?case=${encodeURIComponent(graph.investigation_id)}` : '/live'}
              >
                Open in Live
              </Link>
              <button
                type="button"
                className="btn secondary"
                onClick={() => investigationId && loadGraph(investigationId)}
              >
                Refresh graph
              </button>
            </div>
          </div>

          <div className="net-layout">
            <div className="panel net-canvas">
              <div className="panel-head">
                <h3>Network view</h3>
                <span className="muted mono">{when(graph.generated_at)}</span>
              </div>
              <EntityGraph
                nodes={graph.nodes || []}
                edges={graph.edges || []}
                selectedId={selectedId}
                onSelect={setSelectedId}
                height={440}
              />
              <div className="net-legend">
                {(graph.legend || []).map((l) => (
                  <span key={l.kind} className={`net-legend-item kind-${l.kind}`}>
                    <i />
                    {l.label}
                  </span>
                ))}
                <span className="net-legend-item kind-hidden">
                  <i />
                  Hidden gap
                </span>
              </div>
            </div>

            <aside className="panel net-inspector">
              <div className="panel-head">
                <h3>Object inspector</h3>
              </div>
              {!selectedNode && <p className="muted">Select a node on the graph.</p>}
              {selectedNode && (
                <div className="stack">
                  <div>
                    <span className={`net-kind-pill kind-${selectedNode.kind}`}>
                      {selectedNode.kind}
                    </span>
                    <h4 style={{ margin: '0.45rem 0 0.2rem' }}>{selectedNode.label}</h4>
                    <div className="mono muted">{selectedNode.id}</div>
                  </div>
                  <dl className="net-props">
                    {Object.entries(selectedNode.props || {}).map(([k, v]) => (
                      <div key={k}>
                        <dt>{k}</dt>
                        <dd className="mono">{String(v)}</dd>
                      </div>
                    ))}
                  </dl>
                  {selectedNode.kind === 'player' && selectedNode.props?.player_id && (
                    <Link className="btn" to={`/player/${selectedNode.props.player_id}`}>
                      Open Player 360
                    </Link>
                  )}
                </div>
              )}

              <div className="net-hidden-block">
                <h3>Hidden relations</h3>
                {(graph.hidden_relations || []).length === 0 && (
                  <p className="muted">No predicted gaps for this investigation yet.</p>
                )}
                <ul className="net-hidden-list">
                  {(graph.hidden_relations || []).map((h, i) => (
                    <li key={i}>
                      <strong>{String(h.relation || '').replace(/_/g, ' ')}</strong>
                      <div className="muted">{h.detail}</div>
                      <div className="mono muted">
                        conf {(Number(h.confidence) * 100).toFixed(0)}% · {shortId(h.from)} →{' '}
                        {shortId(h.to)}
                      </div>
                    </li>
                  ))}
                </ul>
              </div>
            </aside>
          </div>

          <div className="panel net-table-panel">
            <div className="panel-head">
              <h3>Entity records</h3>
              <span className="muted">{(graph.records || []).length} rows</span>
            </div>
            <div className="net-table-scroll">
              <table className="table">
                <thead>
                  <tr>
                    <th>Kind</th>
                    <th>Label</th>
                    <th>Entity ID</th>
                    <th>Risk / score</th>
                    <th>Key attr</th>
                  </tr>
                </thead>
                <tbody>
                  {(graph.records || []).map((r) => {
                    const active = r.entity_id === selectedId
                    return (
                      <tr
                        key={r.entity_id}
                        className={active ? 'net-row-active' : undefined}
                        onClick={() => setSelectedId(r.entity_id)}
                        style={{ cursor: 'pointer' }}
                      >
                        <td>
                          <span className={`net-kind-pill kind-${r.kind}`}>{r.kind}</span>
                        </td>
                        <td>{r.label}</td>
                        <td className="mono">{shortId(r.entity_id)}</td>
                        <td className="mono">
                          {r.risk_level || '—'}
                          {r.score != null ? ` · ${Number(r.score).toFixed?.(3) ?? r.score}` : ''}
                        </td>
                        <td className="mono">
                          {r.fingerprint ||
                            r.last4 ||
                            r.location ||
                            r.email ||
                            r.external_ref ||
                            r.player_id ||
                            '—'}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  )
}
