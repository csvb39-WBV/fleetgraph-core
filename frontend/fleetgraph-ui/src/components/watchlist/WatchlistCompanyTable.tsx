import React from 'react';

import type { WatchlistCompanyRecord } from '../../services/watchlistApi';

type Props = {
  companies: WatchlistCompanyRecord[];
  selectedCompanyId: string | null;
  onSelectCompany: (companyId: string) => void;
};

function enrichmentLabel(state: WatchlistCompanyRecord['enrichment_state']): string {
  if (state === 'enriched') {
    return 'Enriched';
  }
  if (state === 'partial') {
    return 'Partial';
  }
  return 'Seed Only';
}

export function WatchlistCompanyTable({ companies, selectedCompanyId, onSelectCompany }: Props): JSX.Element {
  return (
    <section aria-label="Watchlist Company Table">
      <table style={{ width: '100%', borderCollapse: 'collapse', background: '#ffffff', borderRadius: '16px', overflow: 'hidden' }}>
        <thead style={{ background: '#e2e8f0', color: '#0f172a' }}>
          <tr>
            <th style={{ padding: '14px', textAlign: 'left' }}>Company</th>
            <th style={{ padding: '14px', textAlign: 'left' }}>Category</th>
            <th style={{ padding: '14px', textAlign: 'left' }}>Segment</th>
            <th style={{ padding: '14px', textAlign: 'left' }}>Priority Tier</th>
            <th style={{ padding: '14px', textAlign: 'left' }}>Verification</th>
            <th style={{ padding: '14px', textAlign: 'left' }}>Enrichment</th>
          </tr>
        </thead>
        <tbody>
          {companies.map((company) => {
            const isSelected = company.company_id === selectedCompanyId;
            return (
              <tr
                key={company.company_id}
                onClick={() => onSelectCompany(company.company_id)}
                style={{
                  cursor: 'pointer',
                  background: isSelected ? '#eff6ff' : '#ffffff',
                  borderTop: '1px solid #e2e8f0',
                }}
              >
                <td style={{ padding: '14px', fontWeight: 700 }}>{company.company_name}</td>
                <td style={{ padding: '14px' }}>{company.category}</td>
                <td style={{ padding: '14px' }}>{company.segment}</td>
                <td style={{ padding: '14px' }}>{company.priority_tier}</td>
                <td style={{ padding: '14px', textTransform: 'capitalize' }}>{company.verification_status}</td>
                <td style={{ padding: '14px' }}>{enrichmentLabel(company.enrichment_state)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </section>
  );
}

export default WatchlistCompanyTable;
