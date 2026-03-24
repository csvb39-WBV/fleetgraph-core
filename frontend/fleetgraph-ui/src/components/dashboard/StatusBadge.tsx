import type { CSSProperties } from 'react'

interface StatusBadgeProps {
  label: string
  state: 'good' | 'bad' | 'neutral'
}

const styleByState: Record<StatusBadgeProps['state'], CSSProperties> = {
  good: {
    color: '#14532d',
    backgroundColor: '#dcfce7',
    borderColor: '#86efac',
  },
  bad: {
    color: '#7f1d1d',
    backgroundColor: '#fee2e2',
    borderColor: '#fca5a5',
  },
  neutral: {
    color: '#1e3a8a',
    backgroundColor: '#dbeafe',
    borderColor: '#93c5fd',
  },
}

export default function StatusBadge({ label, state }: StatusBadgeProps) {
  return (
    <span
      style={{
        ...styleByState[state],
        display: 'inline-flex',
        alignItems: 'center',
        gap: '8px',
        padding: '6px 10px',
        borderWidth: '1px',
        borderStyle: 'solid',
        borderRadius: '999px',
        fontSize: '12px',
        fontWeight: 700,
        letterSpacing: '0.04em',
        textTransform: 'uppercase',
      }}
    >
      {label}
    </span>
  )
}
