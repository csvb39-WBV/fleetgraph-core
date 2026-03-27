import React from 'react';

import type { TodaySignalsSummary } from '../../services/signalApi';

type Props = {
  summary: TodaySignalsSummary;
  retainedCount: number;
};

function buildCard(label: string, value: number, accent: string): JSX.Element {
  return (
    <article
      key={label}
      style={{
        borderRadius: '16px',
        border: '1px solid #cbd5e1',
        background: '#ffffff',
        padding: '16px',
        boxShadow: '0 8px 24px rgba(15, 23, 42, 0.06)',
      }}
    >
      <div style={{ fontSize: '12px', letterSpacing: '0.08em', textTransform: 'uppercase', color: accent }}>
        {label}
      </div>
      <div style={{ marginTop: '8px', fontSize: '28px', fontWeight: 700, color: '#0f172a' }}>{value}</div>
    </article>
  );
}

export function SignalSummaryCards({ summary, retainedCount }: Props): JSX.Element {
  const cards = [
    { label: 'Primary Signals', value: summary.total_exported_count, accent: '#b45309' },
    { label: 'Retained Signals', value: retainedCount, accent: '#0369a1' },
    { label: 'Litigation', value: summary.count_by_signal_type.litigation, accent: '#991b1b' },
    { label: 'Audit', value: summary.count_by_signal_type.audit, accent: '#1d4ed8' },
    { label: 'Project Distress', value: summary.count_by_signal_type.project_distress, accent: '#92400e' },
    { label: 'Government', value: summary.count_by_signal_type.government, accent: '#166534' },
  ];

  return (
    <section
      aria-label="Signal Summary Cards"
      style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
        gap: '14px',
      }}
    >
      {cards.map((card) => buildCard(card.label, card.value, card.accent))}
    </section>
  );
}

export default SignalSummaryCards;
