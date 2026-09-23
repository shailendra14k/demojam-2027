import { useEffect, useState } from "react";
import { api } from "../api";

export default function Network() {
  const [data, setData] = useState<{ nodes: { id: string; name: string; score: number }[]; edges: { from: string; to: string; type: string }[] }>({ nodes: [], edges: [] });
  useEffect(() => {
    api.network().then(setData).catch(() => undefined);
  }, []);
  return (
    <>
      <div className="page-head"><div><h2>Network</h2><p>Link analysis — correlated cage activity and chip transfer rings</p></div></div>
      <div className="grid-2b">
        <div className="card">
          <div className="card-h"><h3>Entities</h3></div>
          {data.nodes.map((n) => (
            <div className="pipe-item" key={n.id}>
              <span className="grow">{n.name} ({n.id})</span>
              <span className={`score-ring ${n.score >= 85 ? "CRITICAL" : "HIGH"}`}>{n.score}</span>
            </div>
          ))}
        </div>
        <div className="card">
          <div className="card-h"><h3>Relationships</h3></div>
          <table>
            <thead><tr><th>From</th><th>To</th><th>Type</th></tr></thead>
            <tbody>
              {data.edges.map((e, i) => (
                <tr key={i}><td>{e.from}</td><td>{e.to}</td><td>{e.type}</td></tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}
