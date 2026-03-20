import React, { useEffect, useState } from 'react'
import { Navigate, Route, Routes } from 'react-router-dom'
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
  const [uiState, setUiState] = useState('loading')
  const [errorMessage, setErrorMessage] = useState('')
  const [health, setHealth] = useState(null)
  const [summary, setSummary] = useState(null)
  const [records, setRecords] = useState([])
  const [selectedSignalId, setSelectedSignalId] = useState('')

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
    return <main>Loading relationship signal data...</main>
  }

  if (uiState === 'error') {
    return <main>Error loading relationship signal data: {errorMessage}</main>
  }

  if (uiState === 'empty') {
    return <main>No relationship signal records were returned.</main>
  }

  return (
    <Routes>
      <Route path="/" element={<MainLayout />}>
        <Route index element={<Navigate to="/home" replace />} />
        <Route path="home" element={<Home health={health} summary={summary} />} />
        <Route
          path="signals"
          element={
            <Signals
              records={records}
              selectedSignalId={selectedSignalId}
              onSelectSignal={setSelectedSignalId}
            />
          }
        />
        <Route path="summary" element={<Summary summary={summary} records={records} />} />
      </Route>
    </Routes>
  )
}
