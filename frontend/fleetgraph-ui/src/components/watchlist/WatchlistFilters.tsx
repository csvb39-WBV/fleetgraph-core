import React from 'react';

import type { WatchlistFilterState } from '../../services/watchlistApi';

type Props = {
  filters: WatchlistFilterState;
  categories: string[];
  segments: string[];
  priorityTiers: string[];
  verificationStatuses: string[];
  enrichmentStates: string[];
  onChange: (nextFilters: WatchlistFilterState) => void;
};

function renderSelect(
  label: string,
  value: string,
  options: string[],
  onValueChange: (value: string) => void,
): JSX.Element {
  return (
    <label style={{ display: 'grid', gap: '6px', fontSize: '13px', color: '#334155' }}>
      <span style={{ fontWeight: 600 }}>{label}</span>
      <select
        aria-label={label}
        value={value}
        onChange={(event) => onValueChange(event.target.value)}
        style={{
          border: '1px solid #cbd5e1',
          borderRadius: '10px',
          padding: '10px 12px',
          background: '#ffffff',
        }}
      >
        <option value="">All</option>
        {options.map((option) => (
          <option key={option} value={option}>
            {option}
          </option>
        ))}
      </select>
    </label>
  );
}

export function WatchlistFilters({
  filters,
  categories,
  segments,
  priorityTiers,
  verificationStatuses,
  enrichmentStates,
  onChange,
}: Props): JSX.Element {
  return (
    <section
      aria-label="Watchlist Filters"
      style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
        gap: '12px',
        padding: '16px',
        border: '1px solid #d9e2ec',
        borderRadius: '16px',
        background: '#ffffff',
      }}
    >
      {renderSelect('Category Filter', filters.category, categories, (value) => onChange({ ...filters, category: value }))}
      {renderSelect('Segment Filter', filters.segment, segments, (value) => onChange({ ...filters, segment: value }))}
      {renderSelect('Priority Tier Filter', filters.priority_tier, priorityTiers, (value) => onChange({ ...filters, priority_tier: value }))}
      {renderSelect('Verification Status Filter', filters.verification_status, verificationStatuses, (value) => onChange({ ...filters, verification_status: value }))}
      {renderSelect('Enrichment State Filter', filters.enrichment_state, enrichmentStates, (value) => onChange({ ...filters, enrichment_state: value }))}
    </section>
  );
}

export default WatchlistFilters;
