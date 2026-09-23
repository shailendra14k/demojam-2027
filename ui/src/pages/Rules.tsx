import { useEffect, useState } from "react";
import { api } from "../api";

export default function Rules() {
  const [items, setItems] = useState<Record<string, unknown>[]>([]);
  useEffect(() => {
    api.rules().then((r) => setItems(r.items)).catch(() => undefined);
  }, []);
  return (
    <>
      <div className="page-head"><div><h2>Rules</h2><p>Deterministic AML rules evaluated on every enriched event</p></div></div>
      <div className="card table-page">
        <table>
          <thead>
            <tr><th>ID</th><th>Name</th><th>Description</th><th>Severity</th><th>Enabled</th></tr>
          </thead>
          <tbody>
            {items.map((r) => (
              <tr key={String(r.id)}>
                <td className="mono">{String(r.id)}</td>
                <td>{String(r.name)}</td>
                <td style={{ whiteSpace: "normal" }}>{String(r.description)}</td>
                <td><span className={`tag ${r.severity}`}>{String(r.severity)}</span></td>
                <td>{r.enabled ? "Yes" : "No"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
