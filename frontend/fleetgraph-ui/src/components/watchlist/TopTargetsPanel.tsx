import React from 'react';

import type { ContactConfidenceLevel, WatchlistTopTarget } from '../../services/watchlistApi';

type Props = {
  topTargets: WatchlistTopTarget[];
  onSelectCompany: (companyId: string) => void;
};

function priorityBandStyle(priorityBand: WatchlistTopTarget['priority_band']): { background: string; color: string } {
  if (priorityBand === 'HIGH') {
    return { background: '#fee2e2', color: '#991b1b' };
  }
  if (priorityBand === 'MEDIUM') {
    return { background: '#fef3c7', color: '#92400e' };
  }
  return { background: '#e0f2fe', color: '#075985' };
}

function reachabilityLabel(
  reachabilityScore: number | undefined,
  contactConfidenceLevel: ContactConfidenceLevel | undefined,
): string {
  if ((reachabilityScore ?? 0) >= 55 || contactConfidenceLevel === 'high') {
    return 'HIGH REACHABILITY';
  }
  if ((reachabilityScore ?? 0) >= 25 || contactConfidenceLevel === 'medium') {
    return 'MEDIUM REACHABILITY';
  }
  return 'LOW REACHABILITY';
}

export function TopTargetsPanel({ topTargets, onSelectCompany }: Props): JSX.Element {
  return (
    <section aria-label="Top Targets Panel" style={{ border: '1px solid #d9e2ec', borderRadius: '16px', background: '#ffffff', padding: '16px' }}>
      <h3 style={{ marginTop: 0 }}>Top Targets</h3>
      {topTargets.length === 0 ? (
        <p>No top targets are available for this watchlist run.</p>
      ) : (
        <div style={{ display: 'grid', gap: '12px' }}>
          {topTargets.map((target) => {
            const chip = priorityBandStyle(target.priority_band);
            return (
              <button
                key={target.company_id}
                type="button"
                onClick={() => onSelectCompany(target.company_id)}
                style={{
                  border: '1px solid #d9e2ec',
                  borderRadius: '14px',
                  background: '#ffffff',
                  padding: '14px',
                  textAlign: 'left',
                  cursor: 'pointer',
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: '10px', alignItems: 'center', flexWrap: 'wrap' }}>
                  <strong>{target.company_name}</strong>
                  <span style={{ ...chip, borderRadius: '999px', padding: '6px 10px', fontSize: '12px', fontWeight: 700 }}>
                    {target.priority_band} PRIORITY
                  </span>
                </div>
                <div style={{ marginTop: '8px', fontSize: '14px', color: '#334155' }}>{target.reason_summary}</div>
                <div style={{ marginTop: '8px', fontSize: '13px', color: '#475569' }}>
                  Score: {target.priority_score} - State: {target.current_enrichment_state} - Changed: {target.change_detected ? 'Yes' : 'No'}
                </div>
                <div style={{ marginTop: '8px', fontSize: '13px', color: '#1d4ed8', fontWeight: 700 }}>
                  {reachabilityLabel(target.reachability_score, target.contact_confidence_level)}
                </div>
              </button>
            );
          })}
        </div>
      )}
    </section>
  );
}

export default TopTargetsPanel;
