import { useEffect, useState } from "react";
import { api } from "../api";

export default function SAR() {
  const [items, setItems] = useState<Record<string, unknown>[]>([]);
  useEffect(() => {
    api.sars().then((r) => setItems(r.items)).catch(() => undefined);
  }, []);
  return (
    <>
      <div className="page-head"><div><h2>Suspicious Activity Reports</h2><p>SAR drafts queued for compliance review before FinCEN filing</p></div></div>
      <div className="card table-page">
        <table>
          <thead>
            <tr><th>SAR</th><th>Case</th><th>Player</th><th>Amount</th><th>Status</th><th>Narrative</th></tr>
          </thead>
          <tbody>
            {items.map((s) => (
              <tr key={String(s.sarId)}>
                <td className="mono">{String(s.sarId)}</td>
                <td>{String(s.caseId)}</td>
                <td>{String(s.playerName)}</td>
                <td className="mono">${Number(s.amount || 0).toLocaleString()}</td>
                <td><span className="tag HIGH">{String(s.status)}</span></td>
                <td style={{ whiteSpace: "normal" }}>{String(s.narrative)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
