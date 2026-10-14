import { alertDisplayLabel, eventDisplayLabel, severityTitle } from '../lib/alertLabels'

type Alertish = {
  title?: string
  summary?: string
  risk_level?: string
  rule_hits?: unknown[]
  matched_rules?: unknown[]
  display_label?: string
}

type Eventish = {
  risk?: { risk_level?: string; rule_hits?: unknown[] }
  rules?: { hits?: unknown[] }
  ml?: { score?: number }
  behavioral?: { score?: number }
}

export function DetectionBadgeFromAlert({ alert }: { alert: Alertish }) {
  const level = alert.risk_level
  const label = alertDisplayLabel(alert)
  return (
    <span className={`badge detection ${level || ''}`} title={`Severity: ${severityTitle(level)}`}>
      {label}
    </span>
  )
}

export function DetectionBadgeFromEvent({ row }: { row: Eventish }) {
  const level = row.risk?.risk_level
  const label = eventDisplayLabel(row)
  return (
    <span className={`badge detection ${level || ''}`} title={`Severity: ${severityTitle(level)}`}>
      {label}
    </span>
  )
}
