import React from 'react';

import type { WatchlistChangedCompany } from '../../services/watchlistApi';

type Props = {
  changedCompanies: WatchlistChangedCompany[];
  onSelectCompany: (companyId: string) => void;
};

function changeSummary(item: WatchlistChangedCompany): string {
  if (item.change_types.length === 0) {
    return 'No material changes detected.';
  }
  return item.change_types.join(', ');
}

export function ChangedCompaniesPanel({ changedCompanies, onSelectCompany }: Props): JSX.Element {
  return (
    <section aria-label="Changed Companies Panel" style={{ border: '1px solid #d9e2ec', borderRadius: '16px', background: '#ffffff', padding: '16px' }}>
      <h3 style={{ marginTop: 0 }}>Changed Since Last Run</h3>
      {changedCompanies.length === 0 ? (
        <p>No watchlist companies changed since the last comparison.</p>
      ) : (
        <ul style={{ margin: 0, paddingLeft: '18px', display: 'grid', gap: '12px' }}>
          {changedCompanies.map((company) => (
            <li key={company.company_id}>
              <button
                type="button"
                onClick={() => onSelectCompany(company.company_id)}
                style={{
                  border: 'none',
                  background: 'transparent',
                  padding: 0,
                  cursor: 'pointer',
                  textAlign: 'left',
                  color: '#0f172a',
                  font: 'inherit',
                }}
              >
                <strong>{company.company_name}</strong>
              </button>
              <div style={{ fontSize: '14px', color: '#334155', marginTop: '4px' }}>{changeSummary(company)}</div>
              <div style={{ fontSize: '13px', color: '#475569', marginTop: '4px' }}>
                Last refreshed: {company.last_enriched_at || 'Not enriched yet'} - Score: {company.priority_score}
              </div>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

export default ChangedCompaniesPanel;
