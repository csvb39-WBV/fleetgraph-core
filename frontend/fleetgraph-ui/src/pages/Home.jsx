import React from 'react'

export default function Home({ health, summary }) {
  return (
    <section>
      <h1>FleetGraph</h1>
      <p>Deterministic Relationship Intelligence</p>

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
