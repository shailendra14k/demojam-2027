/** Plain-English helpers for Live Monitor Case Detail (non-technical readers). */

import { RULE_NAMES, ruleDisplayName, ruleId } from './alertLabels'

const RULE_MEANING: Record<string, string> = {
  'AML-001':
    'This player made several cash deposits just under the reporting limit. That pattern can look like someone splitting money to avoid paperwork (structuring).',
  'AML-002':
    'A large amount came in, almost none was gambled, then most of it left quickly. That looks more like moving money than playing.',
  'AML-003':
    'Money came in and was taken out again very soon. Fast in-and-out is a common money-laundering pattern.',
  'AML-004':
    'Chips and cage cash moves were mixed in a way that can hide where the money came from.',
  'AML-005':
    'The same phone or device was used by more than one player account. That can mean linked or fake accounts.',
  'AML-006':
    'Activity jumped between places too fast for normal travel. Something about the location story does not add up.',
  'AML-007':
    'This spend is very different from how this player usually behaves.',
  'AML-008':
    'One transaction is much larger than normal for this player.',
  'AML-009':
    'Cash activity in a day reached the amount that must be reported to regulators (CTR).',
  'AML-010':
    'Several cage cash moves sit in the “just under the limit” range — another structuring signal.',
  'AML-011':
    'Many transactions happened in a short time. High speed can mean layering or account sharing.',
  'AML-012':
    'Several different payment methods were used in one day, which can hide the real source of funds.',
  'AML-013':
    'A large wire came in, then cash went out with little real play in between.',
  'AML-014':
    'A large ticket-in / ticket-out (TITO) move was flagged as unusual.',
  'AML-015':
    'This player is on a watchlist or is a politically exposed person (PEP), so extra care is required.',
}

const TX_WORDS: Record<string, string> = {
  CASH_IN: 'cash deposit',
  CASH_OUT: 'cash withdrawal',
  BUY_IN: 'table buy-in',
  WIRE_IN: 'incoming wire',
  WIRE_OUT: 'outgoing wire',
  BLACKJACK: 'blackjack wager',
  ROULETTE: 'roulette wager',
  SLOTS: 'slots play',
  POKER: 'poker wager',
  TITO_ISSUE: 'ticket issue (TITO)',
  TITO_REDEEM: 'ticket redeem (TITO)',
  DEPOSIT: 'deposit',
  WITHDRAWAL: 'withdrawal',
  TRANSACTION: 'transaction',
  CHIP_BUY: 'chip purchase',
  CHIP_CASH: 'chip cash-out',
}

export function friendlyTxType(raw?: string | null): string {
  const key = String(raw || 'TRANSACTION').toUpperCase()
  return TX_WORDS[key] || key.replace(/_/g, ' ').toLowerCase()
}

export function friendlyRisk(level?: string | null): string {
  switch (String(level || '').toUpperCase()) {
    case 'CRITICAL':
      return 'Very high concern — needs attention now'
    case 'HIGH':
      return 'High concern — review soon'
    case 'MEDIUM':
      return 'Medium concern — worth a look'
    case 'LOW':
      return 'Low concern'
    default:
      return 'Risk level not set'
  }
}

export function friendlyStatus(status?: string | null): string {
  switch (String(status || '').toUpperCase()) {
    case 'OPEN':
      return 'New — not reviewed yet'
    case 'INVESTIGATING':
      return 'Under review'
    case 'ESCALATED':
      return 'Sent to a senior investigator'
    case 'FALSE_POSITIVE':
      return 'Closed as a false alarm'
    case 'CLOSED':
      return 'Closed'
    case 'MONITORING':
      return 'Watching for more activity'
    default:
      return String(status || 'Unknown')
  }
}

export function friendlyMoney(amount: unknown, currency = 'USD'): string {
  const n = Number(amount)
  if (!Number.isFinite(n)) return 'an unknown amount'
  try {
    return new Intl.NumberFormat('en-US', { style: 'currency', currency: currency || 'USD' }).format(n)
  } catch {
    return `$${n.toFixed(2)}`
  }
}

export function rulePlainMeaning(hit: any): string {
  const id = ruleId(hit) || ''
  if (id && RULE_MEANING[id]) return RULE_MEANING[id]
  const desc = typeof hit === 'object' && hit?.description ? String(hit.description) : ''
  if (desc && !desc.includes('{') && desc.length < 180) return desc
  const name = ruleDisplayName(hit) || RULE_NAMES[id] || id || 'a compliance check'
  return `Our system flagged this activity under “${name}”.`
}

/** Build a short story anyone can read. */
export function buildPlainStory(input: {
  player_id?: string
  transaction_type?: string
  amount?: number
  currency?: string
  location?: string
  payment_method?: string
  risk_level?: string
  status?: string
  score?: number
  title?: string
  rule_hits?: any[]
  explanations?: string[]
}): {
  headline: string
  in_short: string
  what_happened: string
  why_flagged: string
  what_rules_mean: { name: string; meaning: string }[]
  what_to_do: string
  risk_line: string
  status_line: string
} {
  const player = input.player_id || 'This player'
  const tx = friendlyTxType(input.transaction_type)
  const money = friendlyMoney(input.amount, input.currency)
  const place = input.location && input.location !== 'Casino floor' ? ` at ${input.location}` : ''
  const pay =
    input.payment_method && input.payment_method !== 'UNKNOWN'
      ? ` using ${String(input.payment_method).toLowerCase()}`
      : ''
  const hits = input.rule_hits || []
  const score = Number(input.score ?? 0)
  const risk = String(input.risk_level || 'MEDIUM').toUpperCase()

  const what_rules_mean = hits.slice(0, 6).map((h) => ({
    name: ruleDisplayName(h) || ruleId(h) || 'Rule',
    meaning: rulePlainMeaning(h),
  }))

  const whyParts: string[] = []
  if (hits.length === 1) {
    whyParts.push(rulePlainMeaning(hits[0]))
  } else if (hits.length > 1) {
    whyParts.push(
      `Several checks fired at once (${hits.length}). Together they suggest this activity may not be normal play.`,
    )
  } else {
    whyParts.push(
      'No single named rule stood out, but the overall pattern still looked unusual compared with typical play.',
    )
  }
  if (score >= 0.85) {
    whyParts.push('The overall risk score is very high, so this should not be ignored.')
  } else if (score >= 0.55) {
    whyParts.push('The overall risk score is elevated enough to open a case.')
  }

  let what_to_do: string
  switch (String(input.status || '').toUpperCase()) {
    case 'ESCALATED':
      what_to_do = 'This case was already sent to a senior investigator. Wait for their decision or add notes if you learn more.'
      break
    case 'FALSE_POSITIVE':
      what_to_do = 'This was marked as a false alarm. No further action is needed unless new activity appears.'
      break
    case 'CLOSED':
      what_to_do = 'This case is closed. Re-open only if new related activity shows up.'
      break
    default:
      what_to_do =
        risk === 'CRITICAL' || risk === 'HIGH'
          ? 'Check who this player is, look at linked devices or accounts in Network, then escalate if it still looks suspicious — or mark false positive if it is clearly normal play.'
          : 'Skim the player’s recent activity. Escalate only if something still looks wrong; otherwise you can close or mark false positive.'
  }

  return {
    headline: input.title || 'Suspicious activity case',
    in_short: `${player} did a ${tx} for ${money}. We flagged it as ${friendlyRisk(risk).toLowerCase()}.`,
    what_happened: `${player} completed a ${tx} of ${money}${place}${pay}.`,
    why_flagged: whyParts.join(' '),
    what_rules_mean,
    what_to_do,
    risk_line: friendlyRisk(risk),
    status_line: friendlyStatus(input.status),
  }
}
