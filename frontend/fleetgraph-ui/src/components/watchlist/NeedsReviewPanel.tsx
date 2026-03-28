import React from 'react';

import type { WatchlistNeedsReviewItem } from '../../services/watchlistApi';

type Props = {
  needsReview: WatchlistNeedsReviewItem[];
  onSelectCompany: (companyId: string) => void;
};

export function NeedsReviewPanel({ needsReview, onSelectCompany }: Props): JSX.Element {
  return (
    <section aria-label="Needs Review Panel" style={{ border: '1px solid #d9e2ec', borderRadius: '16px', background: '#ffffff', padding: '16px' }}>
      <h3 style={{ marginTop: 0 }}>Needs Review</h3>
      {needsReview.length === 0 ? (
        <p>No companies currently require review.</p>
      ) : (
        <div style={{ display: 'grid', gap: '12px' }}>
          {needsReview.map((item) => (
            <button
              key={item.company_id}
              type="button"
              onClick={() => onSelectCompany(item.company_id)}
              style={{
                border: '1px solid #d9e2ec',
                borderRadius: '12px',
                background: '#ffffff',
                padding: '14px',
                textAlign: 'left',
                cursor: 'pointer',
              }}
            >
              <strong>{item.company_name}</strong>
              <div style={{ marginTop: '6px', color: '#334155', fontSize: '14px' }}>{item.review_reason_summary}</div>
              <div style={{ marginTop: '6px', color: '#475569', fontSize: '13px' }}>
                State: {item.current_enrichment_state} - Score: {item.priority_score} - Last refreshed: {item.last_enriched_at || 'Not enriched yet'}
              </div>
            </button>
          ))}
        </div>
      )}
    </section>
  );
}

export default NeedsReviewPanel;
