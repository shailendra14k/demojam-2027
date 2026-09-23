import { useEffect, useState } from "react";
import { api } from "../api";

export default function Reports() {
  const [data, setData] = useState<Record<string, unknown> | null>(null);
  useEffect(() => {
    api.reports().then(setData).catch(() => undefined);
  }, []);
  const items = (data?.items as Record<string, unknown>[]) || [];
  return (
    <>
      <div className="page-head"><div><h2>Reports</h2><p>CTR and SAR filing tracker</p></div></div>
      <div className="kpis" style={{ gridTemplateColumns: "repeat(3, 1fr)" }}>
        <article className="kpi"><div className="label">CTRs pending</div><div className="value">{(data?.ctr as { pending?: number })?.pending ?? "—"}</div></article>
        <article className="kpi"><div className="label">SARs pending</div><div className="value">{(data?.sar as { pending?: number })?.pending ?? "—"}</div></article>
        <article className="kpi"><div className="label">Filed today</div><div className="value">{(data?.ctr as { filedToday?: number })?.filedToday ?? "—"}</div></article>
      </div>
      <div className="card table-page">
        <table>
          <thead><tr><th>ID</th><th>Type</th><th>Player</th><th>Amount</th><th>Status</th></tr></thead>
          <tbody>
            {items.map((r) => (
              <tr key={String(r.id)}>
                <td className="mono">{String(r.id)}</td>
                <td>{String(r.type)}</td>
                <td>{String(r.playerId)}</td>
                <td className="mono">${Number(r.amount || 0).toLocaleString()}</td>
                <td>{String(r.status)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
