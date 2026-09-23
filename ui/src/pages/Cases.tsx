import { useEffect, useState } from "react";
import { api } from "../api";
import type { CaseItem } from "../types";

export default function Cases() {
  const [items, setItems] = useState<CaseItem[]>([]);
  useEffect(() => {
    api.cases().then((r) => setItems(r.items)).catch(() => undefined);
  }, []);
  return (
    <>
      <div className="page-head"><div><h2>Cases</h2><p>Investigation queue for the compliance team</p></div></div>
      <div className="card table-page">
        <table>
          <thead>
            <tr><th>Case</th><th>Player</th><th>Title</th><th>Priority</th><th>Status</th><th>Assignee</th></tr>
          </thead>
          <tbody>
            {items.map((c) => (
              <tr key={c.caseId}>
                <td className="mono">{c.caseId}</td>
                <td>{c.playerName}</td>
                <td>{c.title}</td>
                <td><span className={`tag ${c.priority}`}>{c.priority}</span></td>
                <td>{c.status}</td>
                <td>{c.assignee || "Unassigned"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
