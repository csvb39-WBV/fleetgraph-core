import React from 'react'
import { Navigate, Route, Routes } from 'react-router-dom'
import FleetGraphDashboard from './pages/FleetGraphDashboard'

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<FleetGraphDashboard />} />
      <Route path="/dashboard" element={<FleetGraphDashboard />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}
