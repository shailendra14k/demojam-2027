import { useEffect, useState } from 'react'
import { api } from '../api'

/** CTR / SAR filing tracker from live alerts — honest zeros when cold. */
export default function Reports() {
  const [stats, setStats] = useState<any>(null)

  useEffect(() => {
    Promise.all([api.alerts(), api.sars(), api.cases()])
      .then(([alerts, sars, cases]) => {
        const items = alerts.items || []
        const ctr = items.filter((a: any) =>
          (a.rule_hits || []).some((h: any) => h.rule_id === 'AML-009' || String(h.rule_name || '').includes('CTR')),
        ).length
        const critical = items.filter((a: any) => String(a.risk_level).toUpperCase() === 'CRITICAL').length
        setStats({
          alerts: items.length,
          ctrRelated: ctr,
          critical,
          sarDrafts: (sars.items || []).length,
          openCases: (cases.items || []).length,
        })
      })
      .catch(console.error)
  }, [])

  return (
    <section>
      <h2>Reports</h2>
      <p className="muted">Filing tracker from live alert / case / SAR data (no padded demo KPIs).</p>
      {!stats && <p className="muted">Loading…</p>}
      {stats && (
        <ul>
          <li>Open / recent alerts: <strong>{stats.alerts}</strong></li>
          <li>CTR-related (AML-009): <strong>{stats.ctrRelated}</strong></li>
          <li>Critical alerts: <strong>{stats.critical}</strong></li>
          <li>SAR drafts: <strong>{stats.sarDrafts}</strong></li>
          <li>Open cases: <strong>{stats.openCases}</strong></li>
        </ul>
      )}
    </section>
  )
}
