import React from 'react'

export default function Home({ health, summary, demoModeEnabled, demoStage }) {
  return (
    <section style={{ display: 'grid', gap: '16px', color: '#111827' }}>
      <div>
        <h1 style={{ fontSize: '28px', fontWeight: 700, margin: 0 }}>FleetGraph</h1>
        <p style={{ fontSize: '16px', color: '#6b7280', margin: '8px 0 0 0' }}>
          Deterministic Relationship Intelligence
        </p>
      </div>

      {demoModeEnabled ? (
        <section
          style={{
            border: '1px solid #e5e7eb',
            borderRadius: '8px',
            padding: '16px',
            marginBottom: '16px',
            background: '#ffffff',
          }}
        >
          <h2 style={{ fontSize: '18px', fontWeight: 700, margin: 0 }}>Demo Guidance</h2>
          <div style={{ display: 'grid', gap: '8px', marginTop: '12px', color: '#6b7280', fontSize: '14px' }}>
            <p style={{ margin: 0 }}>
              FleetGraph presents deterministic relationship intelligence from a read-only live dataset.
            </p>
            <p style={{ margin: 0 }}>
              Start here to explain the pipeline purpose and confirm the current system state.
            </p>
            <p style={{ margin: 0 }}>Current demo stage: {demoStage}.</p>
            <p style={{ margin: 0 }}>
              Next click: review Signals for record-level exploration or Summary for executive context.
            </p>
          </div>
        </section>
      ) : null}

      <section
        style={{
          border: '1px solid #e5e7eb',
          borderRadius: '8px',
          padding: '16px',
          marginBottom: '16px',
          background: '#ffffff',
        }}
      >
        <h2 style={{ fontSize: '18px', fontWeight: 700, margin: 0 }}>System Overview</h2>
        <div style={{ display: 'grid', gap: '12px', marginTop: '16px' }}>
          <div>
            <div style={{ fontSize: '13px', color: '#6b7280' }}>Backend Status</div>
            <div style={{ fontSize: '15px', color: '#111827', marginTop: '8px' }}>{health?.status}</div>
          </div>
          <div>
            <div style={{ fontSize: '13px', color: '#6b7280' }}>Summary Record Count</div>
            <div style={{ fontSize: '15px', color: '#111827', marginTop: '8px' }}>{summary?.record_count}</div>
          </div>
          <div>
            <div style={{ fontSize: '16px', fontWeight: 700, marginBottom: '12px' }}>Summary Fields</div>
            <ul style={{ margin: 0, paddingLeft: '16px', display: 'grid', gap: '8px' }}>
              <li style={{ fontSize: '14px', color: '#111827' }}>output_type: {summary?.output_type}</li>
              <li style={{ fontSize: '14px', color: '#111827' }}>
                output_schema_version: {summary?.output_schema_version}
              </li>
            </ul>
          </div>
        </div>
      </section>
    </section>
  )
}
