import React from 'react';

import type { WatchlistOutreachQueueItem } from '../../services/watchlistApi';

type Props = {
  items: WatchlistOutreachQueueItem[];
  onSelectCompany: (companyId: string) => void;
};

function queueStatusLabel(item: WatchlistOutreachQueueItem): string {
  if (item.outreach_status === 'suppressed') {
    return 'SUPPRESSED';
  }
  if (item.outreach_status === 'drafted') {
    return 'DRAFTED';
  }
  if (item.readiness_state === 'ready_to_draft') {
    return 'READY TO DRAFT';
  }
  return 'NOT READY';
}

export function OutreachQueuePanel({ items, onSelectCompany }: Props): JSX.Element {
  return (
    <section aria-label="Outreach Queue Panel" style={{ border: '1px solid #d9e2ec', borderRadius: '16px', background: '#ffffff', padding: '16px' }}>
      <h3 style={{ marginTop: 0 }}>Outreach Queue</h3>
      {items.length === 0 ? (
        <p>No outreach-ready companies are currently available.</p>
      ) : (
        <div style={{ display: 'grid', gap: '12px' }}>
          {items.map((item) => (
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
              <div style={{ marginTop: '6px', color: '#334155', fontSize: '14px' }}>
                {item.contact_name || 'No named contact'}{item.contact_email ? ` - ${item.contact_email}` : ''}
              </div>
              <div style={{ marginTop: '6px', color: '#475569', fontSize: '13px' }}>
                Status: {queueStatusLabel(item)}
              </div>
            </button>
          ))}
        </div>
      )}
    </section>
  );
}

export default OutreachQueuePanel;
