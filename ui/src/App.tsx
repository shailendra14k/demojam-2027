import { NavLink, Route, Routes } from 'react-router-dom'
import Dashboard from './pages/Dashboard'
import LiveMonitor from './pages/LiveMonitor'
import Investigation from './pages/Investigation'
import Player360 from './pages/Player360'
import DemoControls from './pages/DemoControls'
import PipelineStatus from './pages/PipelineStatus'
import Cases from './pages/Cases'
import SAR from './pages/SAR'
import Rules from './pages/Rules'
import Settings from './pages/Settings'
import Network from './pages/Network'
import Reports from './pages/Reports'

export default function App() {
  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark" aria-hidden>RH</div>
          <div className="brand-copy">
            <h1>Casino Sentinel</h1>
            <span>Red Hat · OpenShift · AMQ Streams · Debezium · AML</span>
          </div>
        </div>
        <nav className="nav">
          <NavLink to="/" end>Dashboard</NavLink>
          <NavLink to="/live">Live</NavLink>
          <NavLink to="/investigate">Investigate</NavLink>
          <NavLink to="/cases">Cases</NavLink>
          <NavLink to="/sar">SAR</NavLink>
          <NavLink to="/reports">Reports</NavLink>
          <NavLink to="/network">Network</NavLink>
          <NavLink to="/player">Player 360</NavLink>
          <NavLink to="/rules">Rules</NavLink>
          <NavLink to="/demo">Demo</NavLink>
          <NavLink to="/pipeline">Pipeline</NavLink>
          <NavLink to="/settings">Settings</NavLink>
        </nav>
      </header>
      <main className="main">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/live" element={<LiveMonitor />} />
          <Route path="/investigate" element={<Investigation />} />
          <Route path="/cases" element={<Cases />} />
          <Route path="/sar" element={<SAR />} />
          <Route path="/reports" element={<Reports />} />
          <Route path="/network" element={<Network />} />
          <Route path="/network/:investigationId" element={<Network />} />
          <Route path="/player" element={<Player360 />} />
          <Route path="/player/:id" element={<Player360 />} />
          <Route path="/rules" element={<Rules />} />
          <Route path="/demo" element={<DemoControls />} />
          <Route path="/pipeline" element={<PipelineStatus />} />
          <Route path="/settings" element={<Settings />} />
        </Routes>
      </main>
    </div>
  )
}
