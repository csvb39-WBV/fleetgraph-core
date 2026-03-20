import React from 'react'

export default function Header() {
  return (
    <header
      style={{
        borderBottom: '1px solid #e5e7eb',
        padding: '16px 24px',
        background: '#ffffff',
      }}
    >
      <div style={{ fontSize: '24px', fontWeight: 600, color: '#111827' }}>FleetGraph</div>
      <div style={{ fontSize: '14px', color: '#6b7280', marginTop: '8px' }}>
        Deterministic Relationship Intelligence
      </div>
    </header>
  )
}
