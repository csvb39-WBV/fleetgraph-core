import React, { useEffect, useState } from 'react'
import { Navigate, Route, Routes, useLocation } from 'react-router-dom'
import MainLayout from './layouts/MainLayout.jsx'
import Home from './pages/Home.jsx'
import Signals from './pages/Signals.jsx'
import Summary from './pages/Summary.jsx'
import {
  getHealth,
  getRelationshipSignalRecords,
  getRelationshipSignalSummary,
} from './api.js'

export default function App() {
  const location = useLocation()
  const [uiState, setUiState] = useState('loading')
  const [errorMessage, setErrorMessage] = useState('')
  const [health, setHealth] = useState(null)
  const [summary, setSummary] = useState(null)
  const [records, setRecords] = useState([])
  const [selectedSignalId, setSelectedSignalId] = useState('')
  const [demoModeEnabled, setDemoModeEnabled] = useState(true)

  let demoStage = 'overview'
  if (location.pathname === '/signals') {
    demoStage = 'signals'
  } else if (location.pathname === '/summary') {
    demoStage = 'summary'
  }

  useEffect(() => {
    async function load() {
      try {
        const [healthPayload, summaryPayload, recordsPayload] = await Promise.all([
          getHealth(),
          getRelationshipSignalSummary(),
          getRelationshipSignalRecords(),
        ])

        setHealth(healthPayload)
        setSummary(summaryPayload)
        setRecords(recordsPayload)

        if (recordsPayload.length === 0) {
          setSelectedSignalId('')
          setUiState('empty')
          return
        }

        setSelectedSignalId(recordsPayload[0].signal_id)
        setUiState('ready')
      } catch (error) {
        setErrorMessage(error instanceof Error ? error.message : 'API request failed')
        setUiState('error')
      }
    }

    load()
  }, [])

  if (uiState === 'loading') {
    return (
      <main
        style={{
          maxWidth: '1200px',
          margin: '0 auto',
          padding: '24px',
          color: '#111827',
        }}
      >
        Loading relationship signal data...
      </main>
    )
  }

  if (uiState === 'error') {
    return (
      <main
        style={{
          maxWidth: '1200px',
          margin: '0 auto',
          padding: '24px',
          color: '#111827',
        }}
      >
        Error loading relationship signal data: {errorMessage}
      </main>
    )
  }

  if (uiState === 'empty') {
    return (
      <main
        style={{
          maxWidth: '1200px',
          margin: '0 auto',
          padding: '24px',
          color: '#111827',
        }}
      >
        No relationship signal records were returned.
      </main>
    )
  }

  return (
    <div style={{ maxWidth: '1200px', margin: '0 auto', padding: '24px', color: '#111827' }}>
      <section
        style={{
          border: '1px solid #e5e7eb',
          borderRadius: '8px',
          padding: '16px',
          marginBottom: '16px',
          background: '#ffffff',
        }}
      >
        <div style={{ fontSize: '16px', fontWeight: 700, marginBottom: '8px' }}>Demo Mode</div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px', flexWrap: 'wrap' }}>
          <button
            type="button"
            onClick={() => setDemoModeEnabled((current) => !current)}
            style={{
              border: '1px solid #e5e7eb',
              borderRadius: '6px',
              padding: '8px 12px',
              background: '#f3f4f6',
              color: '#111827',
              fontSize: '14px',
              fontWeight: 600,
            }}
          >
            {demoModeEnabled ? 'On' : 'Off'}
          </button>
          <span style={{ color: '#6b7280', fontSize: '14px' }}>Current Stage: {demoStage}</span>
        </div>
      </section>

      <Routes>
        <Route path="/" element={<MainLayout />}>
          <Route index element={<Navigate to="/home" replace />} />
          <Route
            path="home"
            element={
              <Home
                health={health}
                summary={summary}
                demoModeEnabled={demoModeEnabled}
                demoStage={demoStage}
              />
            }
          />
          <Route
            path="signals"
            element={
              <Signals
                records={records}
                selectedSignalId={selectedSignalId}
                onSelectSignal={setSelectedSignalId}
                demoModeEnabled={demoModeEnabled}
                demoStage={demoStage}
              />
            }
          />
          <Route
            path="summary"
            element={
              <Summary
                summary={summary}
                records={records}
                demoModeEnabled={demoModeEnabled}
                demoStage={demoStage}
              />
            }
          />
        </Route>
      </Routes>
    </div>
  )
}
