/** Empty = same-origin (OpenShift nginx proxies /api → casino-api). */
export const API_BASE = (import.meta as any).env?.VITE_API_URL || ''

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...(init?.headers || {}) },
    ...init,
  })
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json()
}

export const api = {
  summary: () => req<any>('/api/v1/metrics/summary'),
  pipeline: () => req<any>('/api/v1/pipeline/status'),
  ready: () => req<any>('/ready'),
  alerts: (status?: string) => req<any>(`/api/v1/alerts${status ? `?status=${status}` : ''}`),
  events: () => req<any>('/api/v1/events/live'),
  player: (id: string) => req<any>(`/api/v1/players/${id}`),
  scenarios: () => req<any>('/api/v1/demo/scenarios'),
  /** Fire-and-poll friendly: unique seed + wait_seconds=0 avoids OpenShift edge 504s. */
  runScenario: (name: string, seed?: number) => {
    const s = seed ?? Math.floor(Date.now() / 1000)
    const q = new URLSearchParams({
      seed: String(s),
      wait_seconds: '0',
    })
    return req<any>(`/api/v1/demo/scenarios/${encodeURIComponent(name)}?${q}`, {
      method: 'POST',
    })
  },
  patchAlert: (id: string, body: any) =>
    req<any>(`/api/v1/alerts/${id}`, { method: 'PATCH', body: JSON.stringify(body) }),
  rulesCatalog: (category?: string) =>
    req<any>(`/api/v1/rules/catalog${category ? `?category=${encodeURIComponent(category)}` : ''}`),
  weights: () => req<any>('/api/v1/settings/weights'),
  patchWeights: (body: {
    rules_pct?: number
    ml_pct?: number
    behavioral_pct?: number
    kyc_pct?: number
    alert_threshold_pct?: number
  }) =>
    req<any>('/api/v1/settings/weights', { method: 'PATCH', body: JSON.stringify(body) }),
  industrySettings: () => req<any>('/api/v1/settings/industry'),
  patchIndustry: (category: string) =>
    req<any>('/api/v1/settings/industry', {
      method: 'PATCH',
      body: JSON.stringify({ category }),
    }),
  simulatorSettings: () => req<any>('/api/v1/settings/simulator'),
  patchSimulatorSettings: (body: { interval_sec?: number; enabled?: boolean }) =>
    req<any>('/api/v1/settings/simulator', { method: 'PATCH', body: JSON.stringify(body) }),
  clearLive: (body?: { seed?: boolean; scenarios?: string[]; pause_simulator?: boolean }) =>
    req<any>('/api/v1/live/clear', {
      method: 'POST',
      body: JSON.stringify(body ?? { seed: false, pause_simulator: true }),
    }),
  caseDetail: (caseId: string) =>
    req<any>(`/api/v1/cases/${encodeURIComponent(caseId)}`),
  networkInvestigations: () => req<any>('/api/v1/network/investigations'),
  investigationNetwork: (id: string) =>
    req<any>(`/api/v1/network/investigations/${encodeURIComponent(id)}`),
}
