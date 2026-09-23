import { useEffect, useState } from "react";
import { api } from "../api";

export default function Transactions() {
  const [items, setItems] = useState<Record<string, unknown>[]>([]);
  useEffect(() => {
    api.transactions().then((r) => setItems(r.items)).catch(() => undefined);
    const id = setInterval(() => api.transactions().then((r) => setItems(r.items)).catch(() => undefined), 4000);
    return () => clearInterval(id);
  }, []);
  return (
    <>
      <div className="page-head"><div><h2>Transactions</h2><p>Operational ledger captured by Debezium CDC</p></div></div>
      <div className="card table-page">
        <table>
          <thead>
            <tr><th>ID</th><th>Player</th><th>Type</th><th>Amount</th><th>Location</th><th>Method</th><th>Time</th></tr>
          </thead>
          <tbody>
            {items.map((t) => (
              <tr key={String(t.transactionId || t.eventId)}>
                <td className="mono">{String(t.transactionId || t.eventId)}</td>
                <td>{String(t.playerId)}</td>
                <td>{String(t.type || t.action)}</td>
                <td className="mono">${Number(t.amount || 0).toLocaleString()}</td>
                <td>{String(t.location || "")}</td>
                <td>{String(t.paymentMethod || "")}</td>
                <td className="mono">{String(t.createdAt || t.time || "").slice(11, 19)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
