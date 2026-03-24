import type { CSSProperties } from 'react'

interface StatCardProps {
  label: string
  value: string | number
}

const containerStyle: CSSProperties = {
  border: '1px solid #cbd5e1',
  borderRadius: '12px',
  padding: '12px',
  background: '#f8fafc',
}

export default function StatCard({ label, value }: StatCardProps) {
  return (
    <div style={containerStyle}>
      <div style={{ fontSize: '12px', fontWeight: 700, color: '#334155', marginBottom: '4px' }}>{label}</div>
      <div style={{ fontSize: '22px', fontWeight: 700, color: '#0f172a', lineHeight: 1.2 }}>{value}</div>
    </div>
  )
}
