/** Human-readable AML detection labels (mirrors casino_common.alert_labels). */

/** Short rule names from RULE_CATALOG — preferred for lists/tables. */
export const RULE_NAMES: Record<string, string> = {
  'AML-001': 'Structuring / Smurfing',
  'AML-002': 'Minimal Gaming',
  'AML-003': 'Rapid Withdrawal',
  'AML-004': 'Chip-Cage Layering',
  'AML-005': 'Shared Device',
  'AML-006': 'Impossible Travel',
  'AML-007': 'Behavioral Deviation',
  'AML-008': 'High-Value Spike',
  'AML-009': 'CTR Threshold',
  'AML-010': 'Cage Structuring',
  'AML-011': 'High Velocity',
  'AML-012': 'Multi-Instrument',
  'AML-013': 'Rapid Wire',
  'AML-014': 'Large TITO',
  'AML-015': 'Watchlist / PEP',
}

const RULE_AI_LABELS: Record<string, string> = {
  'AML-001': 'AI Detected Structuring / Smurfing',
  'AML-002': 'AI Detected Money Laundering',
  'AML-003': 'AI Detected Quick Cash In & Out',
  'AML-004': 'AI Detected Chip-Cage Layering',
  'AML-005': 'AI Detected Shared Device',
  'AML-006': 'AI Detected Impossible Travel',
  'AML-007': 'AI Detected Behavioral Deviation',
  'AML-008': 'AI Detected High-Value Spike',
  'AML-009': 'AI Detected CTR Threshold',
  'AML-010': 'AI Detected Cage Structuring',
  'AML-011': 'AI Detected High Velocity',
  'AML-012': 'AI Detected Multi-Instrument',
  'AML-013': 'AI Detected Rapid Wire Laundering',
  'AML-014': 'AI Detected Large TITO',
  'AML-015': 'AI Detected Watchlist / PEP',
}

const GENERIC_TITLE_RE =
  /^(AML(\s+(CRITICAL|HIGH|MEDIUM|LOW))?(\s*:.*)?|AML-\d{3})$/i

function ruleId(hit: unknown): string {
  if (!hit) return ''
  if (typeof hit === 'string') return hit.trim()
  if (typeof hit === 'object') {
    const h = hit as Record<string, unknown>
    return String(h.rule_id ?? h.id ?? '').trim()
  }
  return ''
}

function labelForRuleId(id: string): string | undefined {
  if (RULE_AI_LABELS[id]) return RULE_AI_LABELS[id]
  const upper = id.toUpperCase()
  if (RULE_AI_LABELS[upper]) return RULE_AI_LABELS[upper]
  return undefined
}

function nameForRuleId(id: string): string | undefined {
  if (!id) return undefined
  if (RULE_NAMES[id]) return RULE_NAMES[id]
  const upper = id.toUpperCase()
  if (RULE_NAMES[upper]) return RULE_NAMES[upper]
  return undefined
}

/** Display name for a rule hit or id string — never prefer raw AML-xxx in the UI. */
export function ruleDisplayName(hit: unknown): string {
  if (!hit) return '—'
  if (typeof hit === 'string') {
    return nameForRuleId(hit) || hit
  }
  if (typeof hit === 'object') {
    const h = hit as Record<string, unknown>
    const name = String(h.rule_name ?? h.name ?? '').trim()
    if (name && !/^AML-\d{3}$/i.test(name)) return name
    const id = ruleId(hit)
    return nameForRuleId(id) || name || id || '—'
  }
  return String(hit)
}

export function formatRuleNames(hits: unknown[] | undefined | null, sep = ', '): string {
  if (!hits?.length) return '—'
  const names: string[] = []
  for (const h of hits) {
    const n = ruleDisplayName(h)
    if (n && n !== '—' && !names.includes(n)) names.push(n)
  }
  return names.join(sep) || '—'
}

function isGenericTitle(title?: string | null): boolean {
  if (!title?.trim()) return true
  const t = title.trim()
  if (t.toUpperCase() === 'AML') return true
  if (GENERIC_TITLE_RE.test(t)) return true
  if (labelForRuleId(t)) return true
  return false
}

export function alertDisplayLabel(alert: {
  title?: string
  summary?: string
  risk_level?: string
  rule_hits?: unknown[]
  matched_rules?: unknown[]
  display_label?: string
  ml_score?: number
  behavioral_score?: number
}): string {
  if (alert.display_label?.trim()) return alert.display_label.trim()
  if (alert.title && !isGenericTitle(alert.title)) return alert.title.trim()

  const ids: string[] = []
  for (const h of alert.rule_hits || []) {
    const id = ruleId(h)
    if (id && !ids.includes(id)) ids.push(id)
  }
  for (const h of alert.matched_rules || []) {
    const id = ruleId(h)
    if (id && !ids.includes(id)) ids.push(id)
  }
  for (const id of ids) {
    const label = labelForRuleId(id)
    if (label) return label
  }

  if (alert.behavioral_score != null && alert.behavioral_score >= 0.65) {
    return 'AI Detected Behavioral Anomaly'
  }
  if (alert.ml_score != null && alert.ml_score >= 0.65) {
    return 'AI Detected ML Fraud Pattern'
  }

  const summary = (alert.summary || '').toLowerCase()
  if (summary.includes('structuring') || summary.includes('smurf')) return RULE_AI_LABELS['AML-001']
  if (summary.includes('minimal') && (summary.includes('gaming') || summary.includes('play'))) {
    return RULE_AI_LABELS['AML-002']
  }
  if (summary.includes('rapid') && (summary.includes('withdraw') || summary.includes('cash'))) {
    return RULE_AI_LABELS['AML-003']
  }
  if (summary.includes('wire')) return RULE_AI_LABELS['AML-013']
  if (summary.includes('watchlist') || summary.includes('pep')) return RULE_AI_LABELS['AML-015']

  const level = String(alert.risk_level || 'HIGH').toUpperCase()
  if (level === 'CRITICAL') return 'AI Detected Critical AML Activity'
  if (level === 'HIGH') return 'AI Detected Elevated AML Risk'
  return 'AI Detected Suspicious Activity'
}

export function eventDisplayLabel(eventRow: {
  risk?: { risk_level?: string; rule_hits?: unknown[]; final_score?: number }
  rules?: { hits?: unknown[] }
  ml?: { score?: number }
  behavioral?: { score?: number }
}): string {
  const risk = eventRow.risk || {}
  const hits = risk.rule_hits || eventRow.rules?.hits || []
  return alertDisplayLabel({
    rule_hits: hits,
    risk_level: risk.risk_level,
    ml_score: eventRow.ml?.score,
    behavioral_score: eventRow.behavioral?.score,
  })
}

export function severityTitle(level?: string): string {
  return String(level || '—').toUpperCase()
}
