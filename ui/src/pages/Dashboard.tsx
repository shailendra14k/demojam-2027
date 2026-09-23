import { useEffect, useMemo, useState } from "react";
import { api } from "../api";
import type { Customer, Kpis, PipelineItem, StreamEvent } from "../types";

function money(n: number) {
  return n.toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });
}

export default function Dashboard() {
  const [kpis, setKpis] = useState<Kpis | null>(null);
  const [events, setEvents] = useState<StreamEvent[]>([]);
  const [pipeline, setPipeline] = useState<PipelineItem[]>([]);
  const [bars, setBars] = useState<{ type: string; count: number }[]>([]);
  const [risk, setRisk] = useState<Customer[]>([]);
  const [busy, setBusy] = useState(false);

  async function refresh() {
    const [k, e, s, b, c] = await Promise.all([
      api.kpis(),
      api.events(),
      api.status(),
      api.alertsByType(),
      api.customers(),
    ]);
    setKpis(k);
    setEvents(e.items);
    setPipeline(s.pipeline);
    setBars(b.items);
    setRisk(c.items.slice(0, 4));
  }

  useEffect(() => {
    refresh().catch(() => undefined);
    const id = setInterval(() => refresh().catch(() => undefined), 2500);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${location.host}/api/v1/ws/live`);
    ws.onmessage = (msg) => {
      try {
        const data = JSON.parse(msg.data);
        if (data.type === "event" && data.item) {
          setEvents((prev) => [data.item, ...prev].slice(0, 40));
        }
      } catch {
        /* ignore */
      }
    };
    return () => ws.close();
  }, []);

  const maxBar = useMemo(() => Math.max(1, ...bars.map((b) => b.count)), [bars]);

  return (
    <>
      <div className="page-head">
        <div>
          <h2>AML Command Center</h2>
          <p>From transaction to suspicion, in seconds — live CDC on AMQ Streams</p>
        </div>
        <div className="row-actions">
          <button className="btn" onClick={() => refresh()}>Refresh</button>
          <button
            className="btn primary"
            disabled={busy}
            onClick={async () => {
              setBusy(true);
              try {
                await api.startScenario("mixed");
                await refresh();
              } catch (err) {
                alert(String(err));
              } finally {
                setBusy(false);
              }
            }}
          >
            {busy ? "Injecting…" : "Start Scenario"}
          </button>
        </div>
      </div>

      <section className="kpis">
        <Kpi label="Transactions Today" value={kpis?.transactionsToday} delta={kpis?.transactionsDelta} suffix="%" />
        <Kpi label="Active Players" value={kpis?.activePlayers} delta={kpis?.activePlayersDelta} suffix="%" />
        <Kpi label="Alerts Today" value={kpis?.alertsToday} delta={kpis?.alertsDelta} abs />
        <Kpi label="Critical Alerts" value={kpis?.criticalAlerts} delta={kpis?.criticalDelta} abs />
        <Kpi label="Open Cases" value={kpis?.openCases} delta={kpis?.openCasesDelta} abs />
        <Kpi label="SARs Pending" value={kpis?.sarsPending} delta={kpis?.sarsDelta} abs />
      </section>

      <section className="grid-2">
        <div className="card">
          <div className="card-h">
            <h3>Real-Time Event Stream</h3>
            <span className="badge">STREAMING ONLINE</span>
          </div>
          <div className="table-page">
            <table>
              <thead>
                <tr>
                  <th>Time</th>
                  <th>User ID</th>
                  <th>Amount</th>
                  <th>Action</th>
                  <th>Location</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {events.slice(0, 12).map((ev) => (
                  <tr key={ev.eventId}>
                    <td className="mono">{ev.time}</td>
                    <td>{ev.playerId}</td>
                    <td className="mono">{money(ev.amount)}</td>
                    <td>{ev.action}</td>
                    <td>{ev.location}</td>
                    <td><span className={`tag ${ev.status}`}>{ev.status}</span></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
        <div className="card">
          <div className="card-h">
            <h3>Live AML Pipeline</h3>
          </div>
          <div className="pipe">
            {(pipeline.length ? pipeline : [
              { id: "1", label: "Casino Transaction", status: "ONLINE" },
              { id: "2", label: "CDC / Debezium", status: "ONLINE" },
              { id: "3", label: "AMQ Streams", status: "ONLINE" },
              { id: "4", label: "Rule Engine", status: "ONLINE" },
              { id: "5", label: "Risk Scoring", status: "ONLINE" },
              { id: "6", label: "Alert", status: "ONLINE" },
              { id: "7", label: "Investigation", status: "ONLINE" },
            ]).map((p) => (
              <div className="pipe-item" key={p.id}>
                <span className="grow">{p.label}</span>
                <span className="badge st">{p.status}</span>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="grid-2b">
        <div className="card">
          <div className="card-h"><h3>Alerts by Type</h3></div>
          <div className="bars">
            {bars.map((b) => (
              <div className="bar-row" key={b.type}>
                <span>{b.type}</span>
                <div className="bar-track">
                  <div className="bar-fill" style={{ width: `${(b.count / maxBar) * 100}%` }} />
                </div>
                <span className="mono">{b.count}</span>
              </div>
            ))}
          </div>
        </div>
        <div className="card">
          <div className="card-h"><h3>Top Risk Customers</h3></div>
          <div className="risk-grid">
            {risk.map((c) => (
              <div className="risk-card" key={c.playerId}>
                <div>
                  <div className="name">{c.name}{c.playerId === "C10281" ? " (Demo)" : ""}</div>
                  <div className="meta">{money(c.monthVolume)} this month · {c.country}</div>
                </div>
                <div className={`score-ring ${c.tier}`}>{c.riskScore}</div>
              </div>
            ))}
          </div>
        </div>
      </section>
    </>
  );
}

function Kpi({ label, value, delta, suffix, abs }: { label: string; value?: number; delta?: number; suffix?: string; abs?: boolean }) {
  const neg = (delta || 0) < 0;
  const shown = abs ? `${neg ? "" : "+"}${delta ?? 0}` : `${neg ? "" : "+"}${delta ?? 0}${suffix || ""}`;
  return (
    <article className="kpi">
      <div className="label">{label}</div>
      <div className="value">{value?.toLocaleString() ?? "—"}</div>
      <div className={`delta ${neg ? "neg" : ""}`}>{shown} vs previous period</div>
    </article>
  );
}
