import { useEffect, useState } from 'react'
import { api } from '../api'

export default function PipelineStatus() {
  const [data, setData] = useState<any>(null)
  useEffect(() => {
    const load = () => api.pipeline().then(setData).catch(console.error)
    load()
    const t = setInterval(load, 4000)
    return () => clearInterval(t)
  }, [])

  const stages = data?.stages || [
    { name: 'DATABASE' }, { name: 'CDC' }, { name: 'STREAM' },
    { name: 'ENRICH' }, { name: 'ANALYZE' }, { name: 'RISK' }, { name: 'ALERT' },
  ]

  return (
    <div>
      <section className="hero">
        <h2>Pipeline status</h2>
        <p>DATABASE → CDC → STREAM → ENRICH → ANALYZE → RISK → ALERT</p>
      </section>
      <div className="panel">
        <div className="pipeline">
          {stages.map((s: any, i: number) => (
            <div key={s.name} style={{display:'contents'}}>
              <div className={`pipe-stage ${s.ok === false ? 'bad' : ''}`}>
                <span className="dot" />
                {s.name}
              </div>
              {i < stages.length - 1 && <span className="pipe-arrow">→</span>}
            </div>
          ))}
        </div>
        <p className="muted" style={{marginTop:'1rem'}}>
          Mode: {data?.pipeline_mode ?? 'unknown'} · Uptime {data?.uptime_seconds ?? 0}s · buffered events {data?.events_buffered ?? 0} · open alerts {data?.open_alerts ?? 0}
        </p>
        {data?.note && <p className="muted">{data.note}</p>}
      </div>
    </div>
  )
}
