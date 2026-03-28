import React from 'react';

import type { WatchlistCompanyRecord } from '../../services/watchlistApi';

type Props = {
  companies: WatchlistCompanyRecord[];
};

export function WatchlistSummaryCards({ companies }: Props): JSX.Element {
  const verifiedCount = companies.filter((company) => company.verification_status === 'verified').length;
  const enrichedCount = companies.filter((company) => company.enrichment_state === 'enriched').length;
  const signalBearingCount = companies.filter((company) => company.recent_signals.length > 0).length;
  const partialCount = companies.filter((company) => company.enrichment_state === 'partial').length;

  const cards = [
    { label: 'Pilot Companies', value: companies.length, accent: '#1d4ed8' },
    { label: 'Verified', value: verifiedCount, accent: '#166534' },
    { label: 'Enriched', value: enrichedCount, accent: '#991b1b' },
    { label: 'Partial', value: partialCount, accent: '#92400e' },
    { label: 'With Signals', value: signalBearingCount, accent: '#7c3aed' },
  ];

  return (
    <section aria-label="Watchlist Summary Cards" style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '12px' }}>
      {cards.map((card) => (
        <article
          key={card.label}
          style={{
            borderRadius: '16px',
            border: '1px solid #d9e2ec',
            background: '#ffffff',
            padding: '16px',
            boxShadow: '0 8px 24px rgba(15, 23, 42, 0.05)',
          }}
        >
          <div style={{ fontSize: '12px', textTransform: 'uppercase', letterSpacing: '0.08em', color: card.accent }}>{card.label}</div>
          <div style={{ marginTop: '8px', fontSize: '28px', fontWeight: 700, color: '#0f172a' }}>{card.value}</div>
        </article>
      ))}
    </section>
  );
}

export default WatchlistSummaryCards;
