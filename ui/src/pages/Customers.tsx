import { useEffect, useState } from "react";
import { api } from "../api";
import type { Customer } from "../types";

function money(n: number) {
  return n.toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });
}

export default function Customers() {
  const [items, setItems] = useState<Customer[]>([]);
  useEffect(() => {
    api.customers().then((r) => setItems(r.items)).catch(() => undefined);
  }, []);
  return (
    <>
      <div className="page-head"><div><h2>Customers</h2><p>Player 360 — fused AML risk profiles</p></div></div>
      <div className="risk-grid">
        {items.map((c) => (
          <div className="risk-card" key={c.playerId} style={{ minHeight: 88 }}>
            <div>
              <div className="name">{c.name}</div>
              <div className="meta">{c.playerId} · {c.country} · {money(c.monthVolume)} / month</div>
            </div>
            <div className={`score-ring ${c.tier}`}>{c.riskScore}</div>
          </div>
        ))}
      </div>
    </>
  );
}
