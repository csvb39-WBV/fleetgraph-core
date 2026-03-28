import React from 'react';

import type { WatchlistSignal } from '../../services/watchlistApi';

type Props = {
  signals: WatchlistSignal[];
};

export function RecentSignalsPanel({ signals }: Props): JSX.Element {
  return (
    <section aria-label="Recent Signals Panel" style={{ border: '1px solid #d9e2ec', borderRadius: '16px', background: '#ffffff', padding: '16px' }}>
      <h3 style={{ marginTop: 0 }}>Recent Signals</h3>
      {signals.length === 0 ? (
        <p>No recent signals have been retained for this company yet.</p>
      ) : (
        <ul style={{ margin: 0, paddingLeft: '18px', display: 'grid', gap: '10px' }}>
          {signals.map((signal) => (
            <li key={`${signal.title}-${signal.source_url}`}>
              <strong>{signal.title}</strong>
              <div style={{ fontSize: '13px', color: '#475569' }}>
                Type: {signal.signal_type} · Status: {signal.status} · Confidence: {signal.confidence}
              </div>
              <div style={{ fontSize: '13px', color: '#475569' }}>Source: {signal.source_url}</div>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

export default RecentSignalsPanel;
