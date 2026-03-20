import React from 'react'
import { Navigate, Route, Routes } from 'react-router-dom'
import MainLayout from './layouts/MainLayout.jsx'
import Home from './pages/Home.jsx'
import Signals from './pages/Signals.jsx'
import Summary from './pages/Summary.jsx'

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<MainLayout />}>
        <Route index element={<Navigate to="/home" replace />} />
        <Route path="home" element={<Home />} />
        <Route path="signals" element={<Signals />} />
        <Route path="summary" element={<Summary />} />
      </Route>
    </Routes>
  )
}
