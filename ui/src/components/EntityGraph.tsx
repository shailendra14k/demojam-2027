import { useEffect, useMemo, useRef, useState, type PointerEvent as ReactPointerEvent } from 'react'

export type GraphNode = {
  id: string
  kind: string
  label: string
  props?: Record<string, unknown>
}

export type GraphEdge = {
  id: string
  source: string
  target: string
  type: string
  hidden?: boolean
  props?: Record<string, unknown>
}

const KIND_COLOR: Record<string, string> = {
  alert: '#f43f5e',
  player: '#38bdf8',
  device: '#f59e0b',
  payment: '#a78bfa',
  location: '#34d399',
  rule: '#2dd4bf',
  watchlist: '#fb7185',
}

type Pos = { x: number; y: number; vx: number; vy: number }

function kindGlyph(kind: string): string {
  switch (kind) {
    case 'alert':
      return '!'
    case 'player':
      return 'P'
    case 'device':
      return 'D'
    case 'payment':
      return '$'
    case 'location':
      return 'L'
    case 'rule':
      return 'R'
    case 'watchlist':
      return 'W'
    default:
      return '·'
  }
}

type Props = {
  nodes: GraphNode[]
  edges: GraphEdge[]
  selectedId: string | null
  onSelect: (id: string) => void
  width?: number
  height?: number
}

/** Lightweight force layout — no extra graph library dependency. */
export default function EntityGraph({
  nodes,
  edges,
  selectedId,
  onSelect,
  width = 860,
  height = 420,
}: Props) {
  const [pos, setPos] = useState<Record<string, Pos>>({})
  const drag = useRef<{ id: string; ox: number; oy: number } | null>(null)
  const frame = useRef(0)

  const nodeIds = useMemo(() => nodes.map((n) => n.id).join('|'), [nodes])
  const edgeKey = useMemo(() => edges.map((e) => e.id).join('|'), [edges])

  useEffect(() => {
    const cx = width / 2
    const cy = height / 2
    const next: Record<string, Pos> = {}
    nodes.forEach((n, i) => {
      const angle = (i / Math.max(nodes.length, 1)) * Math.PI * 2
      const ring = n.kind === 'alert' ? 0 : n.kind === 'player' ? 90 : 150 + (i % 3) * 28
      next[n.id] = {
        x: cx + Math.cos(angle) * ring + (Math.random() - 0.5) * 12,
        y: cy + Math.sin(angle) * ring + (Math.random() - 0.5) * 12,
        vx: 0,
        vy: 0,
      }
    })
    setPos(next)
  }, [nodeIds, width, height])

  useEffect(() => {
    let alive = true
    const tick = () => {
      if (!alive) return
      setPos((prev) => {
        const ids = nodes.map((n) => n.id)
        if (ids.length === 0) return prev
        const p: Record<string, Pos> = { ...prev }
        for (const id of ids) {
          if (!p[id]) p[id] = { x: width / 2, y: height / 2, vx: 0, vy: 0 }
        }
        // Repulsion
        for (let i = 0; i < ids.length; i++) {
          for (let j = i + 1; j < ids.length; j++) {
            const a = p[ids[i]]
            const b = p[ids[j]]
            let dx = a.x - b.x
            let dy = a.y - b.y
            let dist = Math.sqrt(dx * dx + dy * dy) || 0.01
            const force = 900 / (dist * dist)
            dx = (dx / dist) * force
            dy = (dy / dist) * force
            a.vx += dx
            a.vy += dy
            b.vx -= dx
            b.vy -= dy
          }
        }
        // Springs
        for (const e of edges) {
          const a = p[e.source]
          const b = p[e.target]
          if (!a || !b) continue
          let dx = b.x - a.x
          let dy = b.y - a.y
          const dist = Math.sqrt(dx * dx + dy * dy) || 0.01
          const target = e.hidden ? 130 : 100
          const force = (dist - target) * 0.018
          dx = (dx / dist) * force
          dy = (dy / dist) * force
          a.vx += dx
          a.vy += dy
          b.vx -= dx
          b.vy -= dy
        }
        // Center gravity + damp
        const cx = width / 2
        const cy = height / 2
        for (const id of ids) {
          if (drag.current?.id === id) continue
          const n = p[id]
          n.vx += (cx - n.x) * 0.004
          n.vy += (cy - n.y) * 0.004
          n.vx *= 0.82
          n.vy *= 0.82
          n.x = Math.max(28, Math.min(width - 28, n.x + n.vx))
          n.y = Math.max(28, Math.min(height - 28, n.y + n.vy))
        }
        return { ...p }
      })
      frame.current = requestAnimationFrame(tick)
    }
    frame.current = requestAnimationFrame(tick)
    return () => {
      alive = false
      cancelAnimationFrame(frame.current)
    }
  }, [nodeIds, edgeKey, edges, nodes, width, height])

  const onPointerDown = (id: string, ev: ReactPointerEvent) => {
    ev.preventDefault()
    onSelect(id)
    const svg = (ev.target as Element).closest('svg')
    if (!svg) return
    const rect = svg.getBoundingClientRect()
    const n = pos[id]
    drag.current = { id, ox: ev.clientX - rect.left - (n?.x || 0), oy: ev.clientY - rect.top - (n?.y || 0) }
    ;(ev.target as Element).setPointerCapture?.(ev.pointerId)
  }

  const onPointerMove = (ev: ReactPointerEvent) => {
    if (!drag.current) return
    const svg = (ev.target as Element).closest('svg')
    if (!svg) return
    const rect = svg.getBoundingClientRect()
    const { id, ox, oy } = drag.current
    setPos((prev) => ({
      ...prev,
      [id]: {
        ...(prev[id] || { vx: 0, vy: 0 }),
        x: ev.clientX - rect.left - ox,
        y: ev.clientY - rect.top - oy,
        vx: 0,
        vy: 0,
      },
    }))
  }

  const onPointerUp = () => {
    drag.current = null
  }

  return (
    <svg
      className="entity-graph"
      viewBox={`0 0 ${width} ${height}`}
      width="100%"
      height={height}
      onPointerMove={onPointerMove}
      onPointerUp={onPointerUp}
      onPointerLeave={onPointerUp}
    >
      <defs>
        <marker id="arrow" viewBox="0 0 10 10" refX="18" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse">
          <path d="M 0 0 L 10 5 L 0 10 z" fill="rgba(148,163,184,0.55)" />
        </marker>
        <marker id="arrow-hidden" viewBox="0 0 10 10" refX="18" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse">
          <path d="M 0 0 L 10 5 L 0 10 z" fill="rgba(245,158,11,0.85)" />
        </marker>
      </defs>
      {edges.map((e) => {
        const a = pos[e.source]
        const b = pos[e.target]
        if (!a || !b) return null
        return (
          <g key={e.id}>
            <line
              x1={a.x}
              y1={a.y}
              x2={b.x}
              y2={b.y}
              className={e.hidden ? 'graph-edge hidden' : 'graph-edge'}
              markerEnd={e.hidden ? 'url(#arrow-hidden)' : 'url(#arrow)'}
            />
            {e.hidden && (
              <text
                x={(a.x + b.x) / 2}
                y={(a.y + b.y) / 2 - 6}
                className="graph-edge-label"
              >
                {e.type.replace(/_/g, ' ')}
              </text>
            )}
          </g>
        )
      })}
      {nodes.map((n) => {
        const p = pos[n.id]
        if (!p) return null
        const color = KIND_COLOR[n.kind] || '#94a3b8'
        const selected = selectedId === n.id
        return (
          <g
            key={n.id}
            className={`graph-node ${selected ? 'selected' : ''}`}
            transform={`translate(${p.x},${p.y})`}
            onPointerDown={(ev) => onPointerDown(n.id, ev)}
            style={{ cursor: 'grab' }}
          >
            {selected && <circle r={22} className="graph-node-ring" stroke={color} />}
            <circle r={16} fill={color} fillOpacity={0.22} stroke={color} strokeWidth={1.6} />
            <text className="graph-node-glyph" textAnchor="middle" dy="0.35em" fill={color}>
              {kindGlyph(n.kind)}
            </text>
            <text className="graph-node-label" textAnchor="middle" y={28}>
              {n.label.length > 22 ? `${n.label.slice(0, 20)}…` : n.label}
            </text>
          </g>
        )
      })}
    </svg>
  )
}
