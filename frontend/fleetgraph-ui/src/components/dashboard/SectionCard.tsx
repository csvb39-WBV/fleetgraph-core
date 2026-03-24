import type { CSSProperties, ReactNode } from 'react'

interface SectionCardProps {
  title: string
  subtitle?: string
  children: ReactNode
}

const cardStyle: CSSProperties = {
  background: '#ffffff',
  border: '1px solid #e2e8f0',
  borderRadius: '16px',
  padding: '18px',
  boxShadow: '0 10px 30px rgba(15, 23, 42, 0.08)',
}

export default function SectionCard({ title, subtitle, children }: SectionCardProps) {
  return (
    <section style={cardStyle}>
      <header style={{ marginBottom: '14px' }}>
        <h2 style={{ margin: 0, color: '#0f172a', fontSize: '18px', fontWeight: 700 }}>{title}</h2>
        {subtitle ? (
          <p style={{ margin: '6px 0 0', color: '#475569', fontSize: '13px' }}>{subtitle}</p>
        ) : null}
      </header>
      {children}
    </section>
  )
}
