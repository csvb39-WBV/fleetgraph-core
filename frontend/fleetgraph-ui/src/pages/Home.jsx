import React from 'react'

export default function Home({ health, summary, demoModeEnabled, demoStage }) {
  return (
    <section>
      <h1>FleetGraph</h1>
      <p>Deterministic Relationship Intelligence</p>

      {demoModeEnabled ? (
        <section style={{ border: '1px solid #d8d8d8', padding: '0.75rem', marginBottom: '1rem' }}>
          <h2>Demo Guidance</h2>
          <p>FleetGraph presents deterministic relationship intelligence from a read-only live dataset.</p>
          <p>Start here to explain the pipeline purpose and confirm the current system state.</p>
          <p>Current demo stage: {demoStage}.</p>
          <p>Next click: review Signals for record-level exploration or Summary for executive context.</p>
        </section>
      ) : null}

      <h2>System Overview</h2>
      <p>Backend Status: {health?.status}</p>
      <p>Summary Record Count: {summary?.record_count}</p>

      <h3>Summary Fields</h3>
      <ul>
        <li>output_type: {summary?.output_type}</li>
        <li>output_schema_version: {summary?.output_schema_version}</li>
      </ul>
    </section>
  )
}
