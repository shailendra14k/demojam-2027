import { NavLink, Navigate, Route, Routes } from 'react-router-dom'
import Dashboard from './pages/Dashboard'
import LiveMonitor from './pages/LiveMonitor'
import Player360 from './pages/Player360'
import DemoControls from './pages/DemoControls'
import PipelineStatus from './pages/PipelineStatus'
import Rules from './pages/Rules'
import Settings from './pages/Settings'
import Network from './pages/Network'

export default function App() {
  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <NavLink to="/" end className="brand-link" aria-label="Realtime TxnIQ home">
            <img
              className="brand-logo"
              src="/txniq-lockup-light-bg.svg"
              alt="Realtime TxnIQ — The Event-to-Intelligence Blueprint"
            />
          </NavLink>
        </div>
        <nav className="nav">
          <NavLink to="/" end>Dashboard</NavLink>
          <NavLink to="/live">Live</NavLink>
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
          <Route path="/investigate" element={<Navigate to="/live" replace />} />
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
