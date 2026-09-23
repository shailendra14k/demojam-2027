import { NavLink, Outlet, Route, Routes } from "react-router-dom";
import { useEffect, useMemo, useState } from "react";
import { api } from "./api";
import type { StatusPayload } from "./types";
import Dashboard from "./pages/Dashboard";
import Alerts from "./pages/Alerts";
import Cases from "./pages/Cases";
import Customers from "./pages/Customers";
import Transactions from "./pages/Transactions";
import Rules from "./pages/Rules";
import Network from "./pages/Network";
import SAR from "./pages/SAR";
import Reports from "./pages/Reports";
import Settings from "./pages/Settings";

const NAV = [
  { to: "/", label: "Dashboard", icon: "grid" },
  { to: "/alerts", label: "Alerts", icon: "bell" },
  { to: "/cases", label: "Cases", icon: "folder" },
  { to: "/customers", label: "Customers", icon: "users" },
  { to: "/transactions", label: "Transactions", icon: "swap" },
  { to: "/rules", label: "Rules", icon: "list" },
  { to: "/network", label: "Network", icon: "share" },
  { to: "/sar", label: "SAR", icon: "file" },
  { to: "/reports", label: "Reports", icon: "chart" },
];

function Icon({ name }: { name: string }) {
  const common = { fill: "none", stroke: "currentColor", strokeWidth: 1.8, strokeLinecap: "round" as const, strokeLinejoin: "round" as const };
  switch (name) {
    case "grid": return <svg viewBox="0 0 24 24" {...common}><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/></svg>;
    case "bell": return <svg viewBox="0 0 24 24" {...common}><path d="M6 8a6 6 0 1 1 12 0c0 7 3 7 3 9H3c0-2 3-2 3-9"/><path d="M10 21a2 2 0 0 0 4 0"/></svg>;
    case "folder": return <svg viewBox="0 0 24 24" {...common}><path d="M3 7h6l2 2h10v10H3z"/></svg>;
    case "users": return <svg viewBox="0 0 24 24" {...common}><circle cx="9" cy="8" r="3"/><path d="M3 19c0-3 2.5-5 6-5s6 2 6 5"/><circle cx="17" cy="9" r="2"/><path d="M21 19c0-2-1.5-3.5-4-4"/></svg>;
    case "swap": return <svg viewBox="0 0 24 24" {...common}><path d="M7 7h13l-3-3"/><path d="M17 17H4l3 3"/></svg>;
    case "list": return <svg viewBox="0 0 24 24" {...common}><path d="M8 6h13M8 12h13M8 18h13M3 6h.01M3 12h.01M3 18h.01"/></svg>;
    case "share": return <svg viewBox="0 0 24 24" {...common}><circle cx="6" cy="6" r="2"/><circle cx="18" cy="6" r="2"/><circle cx="12" cy="18" r="2"/><path d="M8 7l8 0M7 8l4 8M17 8l-4 8"/></svg>;
    case "file": return <svg viewBox="0 0 24 24" {...common}><path d="M14 3H6v18h12V9z"/><path d="M14 3v6h6"/></svg>;
    case "chart": return <svg viewBox="0 0 24 24" {...common}><path d="M4 19V5"/><path d="M4 19h16"/><path d="M8 16v-6"/><path d="M12 16V8"/><path d="M16 16v-9"/></svg>;
    default: return <svg viewBox="0 0 24 24" {...common}><circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M2 12h3M19 12h3"/></svg>;
  }
}

function Layout() {
  const [status, setStatus] = useState<StatusPayload | null>(null);
  const [live, setLive] = useState(true);
  const [menu, setMenu] = useState(false);
  const dateLabel = useMemo(
    () =>
      new Date().toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" }) + " - DEMO",
    []
  );

  useEffect(() => {
    api.status().then(setStatus).catch(() => undefined);
    const id = setInterval(() => api.status().then(setStatus).catch(() => undefined), 8000);
    return () => clearInterval(id);
  }, []);

  const dbLabel = status?.database || "MOCK FALLBACK";
  const dbWarn = dbLabel.includes("MOCK") || dbLabel.includes("FALLBACK");

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">CG</div>
          <div>
            <h1>CASINO GUARD</h1>
            <p>REAL-TIME AML</p>
          </div>
        </div>
        <nav className="nav">
          {NAV.map((n) => (
            <NavLink key={n.to} to={n.to} end={n.to === "/"} className={({ isActive }) => (isActive ? "active" : "")}>
              <Icon name={n.icon} />
              <span>{n.label}</span>
            </NavLink>
          ))}
          <div className="nav-label">System</div>
          <NavLink to="/settings" className={({ isActive }) => (isActive ? "active" : "")}>
            <Icon name="settings" />
            <span>Settings</span>
          </NavLink>
        </nav>
        <div className="side-status">
          <div className="dot-row"><span className="dot" /> Streaming Online</div>
          <div className="dot-row"><span className="dot" /> Rule Engine Online</div>
          <div className="dot-row"><span className={`dot ${dbWarn ? "warn" : ""}`} /> Database {dbWarn ? "Fallback" : "Connected"}</div>
        </div>
      </aside>
      <div className="main">
        <header className="topbar">
          <label className="search">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="11" cy="11" r="7"/><path d="M20 20l-3-3"/></svg>
            <input placeholder="Search alerts, customers, transactions." />
          </label>
          <div className="top-actions">
            <span className="date-chip">{dateLabel}</span>
            <button className="icon-btn" aria-label="Notifications">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M6 8a6 6 0 1 1 12 0c0 7 3 7 3 9H3c0-2 3-2 3-9"/><path d="M10 21a2 2 0 0 0 4 0"/></svg>
            </button>
            <div className="toggle">
              Live Mode
              <button className={`switch ${live ? "on" : ""}`} onClick={() => setLive(!live)} aria-label="Live mode">
                <span />
              </button>
            </div>
            <button className="demo-pill" onClick={() => setMenu(!menu)}>DEMO</button>
            <div className="user">
              <div className="avatar" />
              {status?.user.name || "Sarah Wilson"}
            </div>
          </div>
        </header>
        {menu && (
          <div className="menu">
            {["structuring", "rapid-wire", "minimal-play", "high-velocity", "watchlist", "mixed"].map((s) => (
              <button
                key={s}
                onClick={() => {
                  setMenu(false);
                  api.startScenario(s).catch((e) => alert(String(e)));
                }}
              >
                Start scenario: {s}
              </button>
            ))}
          </div>
        )}
        <div className="status-strip">
          <span className="env">DEMO ENVIRONMENT</span>
          <span>Streaming: <b>{status?.streaming || "ONLINE"}</b></span>
          <span>Debezium: <b>{status?.debezium || "ONLINE"}</b></span>
          <span>AMQ Streams: <b>{status?.amqStreams || "ONLINE"}</b></span>
          <span>Rule Engine: <b>{status?.ruleEngine || "ONLINE"}</b></span>
          <span>Database: <b className={dbWarn ? "warn" : ""}>{dbLabel}</b></span>
        </div>
        <div className="content">
          <Outlet context={{ live }} />
        </div>
      </div>
    </div>
  );
}

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<Dashboard />} />
        <Route path="/alerts" element={<Alerts />} />
        <Route path="/cases" element={<Cases />} />
        <Route path="/customers" element={<Customers />} />
        <Route path="/transactions" element={<Transactions />} />
        <Route path="/rules" element={<Rules />} />
        <Route path="/network" element={<Network />} />
        <Route path="/sar" element={<SAR />} />
        <Route path="/reports" element={<Reports />} />
        <Route path="/settings" element={<Settings />} />
      </Route>
    </Routes>
  );
}
