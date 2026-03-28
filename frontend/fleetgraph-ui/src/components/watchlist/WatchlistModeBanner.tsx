import React from 'react';

export function WatchlistModeBanner(): JSX.Element {
  return (
    <section
      aria-label="Watchlist Mode Banner"
      style={{
        border: '1px solid #bfdbfe',
        background: '#eff6ff',
        borderRadius: '16px',
        padding: '16px',
        display: 'grid',
        gap: '6px',
      }}
    >
      <div style={{ fontSize: '12px', textTransform: 'uppercase', letterSpacing: '0.08em', color: '#1d4ed8', fontWeight: 700 }}>
        Watchlist Mode
      </div>
      <div style={{ fontSize: '18px', fontWeight: 700, color: '#0f172a' }}>
        Company-first monitoring for the verified pilot watchlist.
      </div>
      <div style={{ color: '#334155', lineHeight: 1.5 }}>
        Discovery Mode remains separate. This console is dedicated to seeded companies, source-backed enrichment, and operator review of partial or completed company records.
      </div>
    </section>
  );
}

export default WatchlistModeBanner;
