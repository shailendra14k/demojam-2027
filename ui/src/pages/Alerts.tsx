import { useEffect, useState } from "react";
import { api } from "../api";
import type { AlertItem } from "../types";

export default function Alerts() {
  const [items, setItems] = useState<AlertItem[]>([]);
  useEffect(() => {
    api.alerts().then((r) => setItems(r.items)).catch(() => undefined);
    const id = setInterval(() => api.alerts().then((r) => setItems(r.items)).catch(() => undefined), 4000);
    return () => clearInterval(id);
  }, []);
  return (
    <>
      <div className="page-head"><div><h2>Alerts</h2><p>Fused rule + ML + behavioral + KYC scores</p></div></div>
      <div className="card table-page">
        <table>
          <thead>
            <tr>
              <th>Time</th><th>Player</th><th>Type</th><th>Priority</th><th>Score</th><th>Explanation</th>
            </tr>
          </thead>
          <tbody>
            {items.map((a) => (
              <tr key={a.alertId}>
                <td className="mono">{a.createdAt?.slice(11, 19)}</td>
                <td>{a.playerName} <span className="meta">({a.playerId})</span></td>
                <td><span className={`tag ${a.alertType}`}>{a.alertType}</span></td>
                <td><span className={`tag ${a.priority}`}>{a.priority}</span></td>
                <td className="mono">{a.riskScore}</td>
                <td style={{ whiteSpace: "normal", maxWidth: 480 }}>{a.explanation}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
