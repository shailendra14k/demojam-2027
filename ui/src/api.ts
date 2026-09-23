import type { AlertItem, CaseItem, Customer, Kpis, StatusPayload, StreamEvent } from "./types";

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`${path} ${res.status}`);
  return res.json() as Promise<T>;
}

export const api = {
  status: () => getJson<StatusPayload>("/api/v1/status"),
  kpis: () => getJson<Kpis>("/api/v1/kpis"),
  events: () => getJson<{ items: StreamEvent[] }>("/api/v1/events"),
  alerts: () => getJson<{ items: AlertItem[] }>("/api/v1/alerts"),
  alertsByType: () => getJson<{ items: { type: string; count: number }[] }>("/api/v1/alerts/by-type"),
  customers: () => getJson<{ items: Customer[] }>("/api/v1/customers"),
  cases: () => getJson<{ items: CaseItem[] }>("/api/v1/cases"),
  sars: () => getJson<{ items: Record<string, unknown>[] }>("/api/v1/sars"),
  rules: () => getJson<{ items: Record<string, unknown>[] }>("/api/v1/rules"),
  transactions: () => getJson<{ items: Record<string, unknown>[] }>("/api/v1/transactions"),
  network: () => getJson<{ nodes: { id: string; name: string; score: number }[]; edges: { from: string; to: string; type: string }[] }>("/api/v1/network"),
  reports: () => getJson<Record<string, unknown>>("/api/v1/reports"),
  startScenario: async (name: string) => {
    const res = await fetch(`/api/v1/demo/scenarios/${name}`, { method: "POST" });
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  },
};
